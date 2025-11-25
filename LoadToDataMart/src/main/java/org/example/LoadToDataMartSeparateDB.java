package org.example;

import javax.swing.SwingUtilities;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Properties;

public class LoadToDataMartSeparateDB {

    // Không còn các biến tĩnh SRC_URL, MART_URL ở đây nữa.
    // Cấu hình được lưu trong CsvConfigLoader.

    private static boolean configLoaded = false; // Biến cờ để kiểm tra cấu hình đã load chưa

    /* ===================== CONFIG & CONN ===================== */

    // Đã sửa: load config bằng cách hiển thị pop-up và lưu vào CsvConfigLoader
    private static void loadConfig() throws Exception {
        if (configLoaded) return;

        // Chạy logic GUI (CsvConfigLoader.loadConfigFromCsv) trên Event Dispatch Thread (EDT)
        SwingUtilities.invokeAndWait(() -> {
            try {
                // Phương thức này là void và sẽ hiển thị pop-up chọn file
                // Nó lưu kết quả vào biến tĩnh của CsvConfigLoader.
                CsvConfigLoader.loadConfigFromCsv();
            } catch (Exception e) {
                // Ném ngoại lệ đã bắt ra bên ngoài invokeAndWait
                throw new RuntimeException(e);
            }
        });

        // Cấu hình đã được lưu thành công vào CsvConfigLoader
        configLoaded = true;
    }

    private static Connection srcConn() throws Exception {
        if (!configLoaded) loadConfig();
        // Lấy thông tin kết nối từ CsvConfigLoader
        return DriverManager.getConnection(
                CsvConfigLoader.SRC_URL,
                CsvConfigLoader.SRC_USER,
                CsvConfigLoader.SRC_PASS
        );
    }

    private static Connection martConn() throws Exception {
        if (!configLoaded) loadConfig();
        // Lấy thông tin kết nối từ CsvConfigLoader
        return DriverManager.getConnection(
                CsvConfigLoader.MART_URL,
                CsvConfigLoader.MART_USER,
                CsvConfigLoader.MART_PASS
        );
    }

    // (7.2 Kiểm tra DB Mart? & 7.3 Tạo Database mới)
    private static void ensureMartDB() {
        try {
            // Lấy URL từ CsvConfigLoader
            if (!CsvConfigLoader.MART_URL.toLowerCase().startsWith("jdbc:mysql:")) return;

            String urlNoParams = CsvConfigLoader.MART_URL.split("\\?")[0];
            int lastSlash = urlNoParams.lastIndexOf('/');
            if (lastSlash < 0 || lastSlash == urlNoParams.length() - 1) return;

            String dbName = urlNoParams.substring(lastSlash + 1);
            String baseUrl = urlNoParams.substring(0, lastSlash);

            try (Connection c = DriverManager.getConnection(baseUrl, CsvConfigLoader.MART_USER, CsvConfigLoader.MART_PASS);
                 Statement st = c.createStatement()) {
                st.executeUpdate("CREATE DATABASE IF NOT EXISTS `" + dbName + "` " +
                        "CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci");
            }
        } catch (Exception ignore) {}
    }
    /* ========================================================= */

    public static void main(String[] args) {
        System.out.println("=== Load Data Mart: db_warehouse → db_mart ===");
// BƯỚC 7.1 & 7.2 & 7.3: Đọc Config, Kết nối & Đảm bảo DB Mart tồn tại
        try {
            loadConfig();
            // 7.2 Kiểm tra DB Mart? & 7.3 Tạo Database mới (Logic nằm trong ensureMartDB)
            ensureMartDB();
        } catch (Exception e) {
            // Xử lý exception từ invokeAndWait hoặc CsvConfigLoader
            Throwable cause = e;
            if (e instanceof java.lang.reflect.InvocationTargetException) {
                cause = e.getCause() != null ? e.getCause() : e;
            }
            System.err.println("Lỗi cấu hình hoặc không chọn file: " + cause.getMessage());
            cause.printStackTrace();
            return;
        }

        // BƯỚC 7.4 & 7.5: Tạo schema + full refresh
        // Logic thực hiện 7.4 (Drop bảng cũ) và 7.5 (Create bảng mới & Index) nằm trong hàm này
        if (!recreateMartSchema()) return;

        // BƯỚC 7.6 & 7.7: Nạp dm_brand_performance
        int rowsBrand = loadBrandPerformance();
        if (rowsBrand < 0) return;

        // BƯỚC 7.8 & 7.9: Nạp dim_date
        int rowsDate = loadDimDate();
        if (rowsDate < 0) return;

        // BƯỚC 7.10: Tạo view
        createViewSafe();

        System.out.println("→ OK. Chèn " + rowsBrand + " dòng vào db_mart.dm_brand_performance.");
        System.out.println("→ OK. Chèn " + rowsDate + " dòng vào db_mart.dim_date.");
        System.out.println("→ View vw_brand đã sẵn sàng trong db_mart.");
    }

    /* ===================== SCHEMA & VIEW ===================== */
    private static boolean recreateMartSchema() {
        // BƯỚC 7.4: Drop bảng cũ: dm_brand_performance, dim_date
        try (Connection mart = martConn(); Statement st = mart.createStatement()) {
            mart.setAutoCommit(false);

            // Tắt FK check để drop an toàn nếu mở rộng thêm
            st.execute("SET FOREIGN_KEY_CHECKS=0");
            st.executeUpdate("DROP TABLE IF EXISTS dm_brand_performance");
            st.executeUpdate("DROP TABLE IF EXISTS dim_date");
            st.execute("SET FOREIGN_KEY_CHECKS=1");

            // BƯỚC 7.5: Create bảng mới & Index
            st.executeUpdate(
                    "CREATE TABLE dm_brand_performance (" +
                            "  brandName     VARCHAR(100) PRIMARY KEY," +
                            "  averagePrice  DECIMAL(15,2)," +
                            "  averageRating FLOAT," +
                            "  phoneCount    INT" +
                            ")"
            );

            st.executeUpdate(
                    "CREATE TABLE dim_date (" +
                            "  date_key INT PRIMARY KEY," +
                            "  full_date DATE," +
                            "  day_of_week VARCHAR(10)," +
                            "  day_of_month INT," +
                            "  day_of_year INT," +
                            "  month_name VARCHAR(10)," +
                            "  month_of_year INT," +
                            "  quarter_of_year VARCHAR(2)," +
                            "  year INT" +
                            ")"
            );

            safeCreateIndex(st, "CREATE INDEX idx_dm_brand_avgPrice  ON dm_brand_performance(averagePrice)");
            safeCreateIndex(st, "CREATE INDEX idx_dm_brand_avgRating ON dm_brand_performance(averageRating)");

            mart.commit();
            return true;
        } catch (Exception e) {
            System.err.println("Lỗi tạo schema db_mart: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    private static void safeCreateIndex(Statement st, String sql) {
        try { st.executeUpdate(sql); } catch (Exception ignore) {}
    }
    // BƯỚC 7.10: Tạo View: vw_brand
    private static void createViewSafe() {
        try (Connection mart = martConn(); Statement st = mart.createStatement()) {
            try { st.executeUpdate("DROP VIEW IF EXISTS vw_brand"); } catch (Exception ignore) {}
            st.executeUpdate(
                    "CREATE VIEW vw_brand AS " +
                            "SELECT brandName, averagePrice, averageRating, phoneCount " +
                            "FROM dm_brand_performance"
            );
        } catch (Exception e) {
            System.err.println("Cảnh báo: tạo view thất bại: " + e.getMessage());
        }
    }
    /* ========================================================= */

    /* ===================== LOAD BRAND ======================== */
    private static int loadBrandPerformance() {
        final String selAgg =
                "SELECT brandName, averagePrice, averageRating, phoneCount FROM agg_brand_summary";
        final String insBrand =
                "INSERT INTO dm_brand_performance (brandName, averagePrice, averageRating, phoneCount) VALUES (?,?,?,?)";

        int totalInserted = 0;
        try (Connection src = srcConn();
             Connection mart = martConn();
             PreparedStatement ps = mart.prepareStatement(insBrand);
             Statement s = src.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {

            // Streaming cho MySQL
            try { s.setFetchSize(Integer.MIN_VALUE); } catch (Exception ignore) {}
            try { src.setAutoCommit(false); } catch (Exception ignore) {}
            mart.setAutoCommit(false);
// BƯỚC 7.6: Đọc agg_brand_summary từ Warehouse
            ResultSet rs = s.executeQuery(selAgg);
            final int BATCH = 1000;
            int pending = 0;
// BƯỚC 7.7: Ghi vào dm_brand_performance tại Data Mart
            while (rs.next()) {
                // brandName
                ps.setString(1, rs.getString("brandName"));

                // averagePrice null-safe
                BigDecimal avgPrice = rs.getBigDecimal("averagePrice");
                if (avgPrice != null) ps.setBigDecimal(2, avgPrice);
                else ps.setNull(2, Types.DECIMAL);

                // averageRating null-safe
                float avgRating = rs.getFloat("averageRating");
                if (rs.wasNull()) ps.setNull(3, Types.FLOAT);
                else ps.setFloat(3, avgRating);

                // phoneCount null-safe
                int cnt = rs.getInt("phoneCount");
                if (rs.wasNull()) ps.setNull(4, Types.INTEGER);
                else ps.setInt(4, cnt);

                ps.addBatch();
                pending++;

                if (pending % BATCH == 0) {
                    totalInserted = flushBatch(ps, pending, totalInserted);
                    pending = 0;
                }
            }
            totalInserted = flushBatch(ps, pending, totalInserted);
            mart.commit();
            return totalInserted;

        } catch (Exception e) {
            System.err.println("Lỗi nạp dm_brand_performance: " + e.getMessage());
            e.printStackTrace();
            return -1;
        }
    }
    /* ========================================================= */

    /* ===================== LOAD DIM_DATE ===================== */
    private static int loadDimDate() {
        final String selDate =
                "SELECT date_key, full_date, day_of_week, day_of_month, day_of_year, " +
                        "       month_name, month_of_year, quarter_of_year, year " +
                        "FROM dim_date";

        final String insDate =
                "INSERT INTO dim_date (" +
                        "  date_key, full_date, day_of_week, day_of_month, day_of_year, " +
                        "  month_name, month_of_year, quarter_of_year, year" +
                        ") VALUES (?,?,?,?,?,?,?,?,?)";

        int totalInserted = 0;
        try (Connection src = srcConn();
             Connection mart = martConn();
             PreparedStatement ps = mart.prepareStatement(insDate);
             Statement s = src.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {

            // Streaming cho MySQL
            try { s.setFetchSize(Integer.MIN_VALUE); } catch (Exception ignore) {}
            try { src.setAutoCommit(false); } catch (Exception ignore) {}
            mart.setAutoCommit(false);
// BƯỚC 7.8: Đọc dim_date từ Warehouse
            ResultSet rs = s.executeQuery(selDate);
            final int BATCH = 1000;
            int pending = 0;
// BƯỚC 7.9: Ghi vào dim_date tại Data Mart
            while (rs.next()) {
                ps.setInt(1, rs.getInt("date_key"));

                Date d = rs.getDate("full_date");
                if (d != null) ps.setDate(2, d);
                else ps.setNull(2, Types.DATE);

                ps.setString(3, rs.getString("day_of_week"));
                ps.setInt(4, rs.getInt("day_of_month"));
                ps.setInt(5, rs.getInt("day_of_year"));
                ps.setString(6, rs.getString("month_name"));
                ps.setInt(7, rs.getInt("month_of_year"));
                ps.setString(8, rs.getString("quarter_of_year"));
                ps.setInt(9, rs.getInt("year"));

                ps.addBatch();
                pending++;

                if (pending % BATCH == 0) {
                    totalInserted = flushBatch(ps, pending, totalInserted);
                    pending = 0;
                }
            }
            totalInserted = flushBatch(ps, pending, totalInserted);
            mart.commit();
            return totalInserted;

        } catch (Exception e) {
            System.err.println("Lỗi nạp dim_date: " + e.getMessage());
            e.printStackTrace();
            return -1;
        }
    }
    /* ========================================================= */

    /* ===================== UTIL ============================== */
    // Đếm batch theo số record addBatch đã đẩy, không phụ thuộc SUCCESS_NO_INFO
    private static int flushBatch(PreparedStatement ps, int pending, int total) throws SQLException {
        if (pending == 0) return total;
        ps.executeBatch();
        return total + pending;
    }
    /* ========================================================= */
}