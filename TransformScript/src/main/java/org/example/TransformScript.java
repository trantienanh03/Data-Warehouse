package org.example;

import java.sql.*;
import java.util.*;

// Thêm import cho CsvConfigLoader


public class TransformScript {

    // --- Thông tin kết nối CỐ ĐỊNH được loại bỏ/comment ---
    // private static final String JDBC_URL = "jdbc:mysql://localhost:3306/db_staging";
    // private static final String JDBC_USER = "root";
    // private static final String JDBC_PASSWORD = "1234";

    static final String[] COLS = {
            "name","screenSize","screenTechnology","screenResolution","camera","chipset",
            "ram","storage","battery","version","color","price","oldPrice","link",
            "rating","numReviews","nfc","releaseDate"
    };

    // Cột bắt buộc (thiếu → loại)
    static final Set<String> REQUIRED = Set.of("name","price","link");

    // Không dùng ngưỡng completeness nữa (để 0 cho rõ)
    static final double COMPLETENESS_THRESHOLD = 0.0;

    public static void main(String[] args) throws Exception {
        // 1. Tải cấu hình từ file CSV (sẽ mở cửa sổ chọn file)
        CsvConfigLoader.loadConfigFromCsv();


        String url  = CsvConfigLoader.SRC_URL;
        String user = CsvConfigLoader.SRC_USER;
        String pass = CsvConfigLoader.SRC_PASS;
// BƯỚC 3.1: Kết nối cơ sở dữ liệu (db_staging)
        try (Connection conn = DriverManager.getConnection(url, user, pass)) {
            conn.setAutoCommit(false);

            // BƯỚC 3.2: Đọc toàn bộ dữ liệu từ bảng nguồn (stg_phones)
            List<Map<String,Object>> rows = readAll(conn);

            // BƯỚC 3.3: Lọc tối thiểu (cleanMinimal)
            List<Map<String,Object>> cleaned = cleanMinimal(rows);

            // BƯỚC 3.4 & 3.5: Sao lưu và Ghi đè dữ liệu đã làm sạch
            backupAndReplace(conn, cleaned);

            conn.commit();
            System.out.println("Done. Input: " + rows.size() + " rows; Output: " + cleaned.size() + " rows.");
        }
        // BƯỚC 3.1.1: Thông báo lỗi (Thất bại)
        // Nếu DriverManager.getConnection() (3.1) thất bại (sai URL/User/Pass/DB down),
        // nó sẽ ném ra một SQLException.
        // Do hàm main khai báo 'throws Exception', ngoại lệ này sẽ làm chương trình
        // DỪNG LẠI ngay lập tức và in chi tiết lỗi (stack trace) ra console,
        // tương ứng với hành động "Dừng tiến trình, in thông báo lỗi" của 3.1.1.
    }
    // 3.2 Logic đọc toàn bộ dữ liệu từ stg_phones
    private static List<Map<String,Object>> readAll(Connection conn) throws SQLException {
        String sql = "SELECT * FROM stg_phones";
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            List<Map<String,Object>> out = new ArrayList<>();
            ResultSetMetaData md = rs.getMetaData();
            int n = md.getColumnCount();
            while (rs.next()) {
                Map<String,Object> row = new LinkedHashMap<>();
                for (int i=1; i<=n; i++) {
                    String col = md.getColumnLabel(i);
                    Object val = rs.getObject(i);
                    row.put(col, val);
                }
                out.add(row);
            }
            return out;
        }
    }

    // 3.3 Logic Lọc tối thiểu (cleanMinimal)
    private static List<Map<String,Object>> cleanMinimal(List<Map<String,Object>> rows) {
        List<Map<String,Object>> out = new ArrayList<>(rows.size());
        for (Map<String,Object> r : rows) {
            if (hasEmptyRequired(r)) continue; // chỉ điều kiện duy nhất
            out.add(r);
        }
        return out;
    }

    private static boolean hasEmptyRequired(Map<String,Object> r) {
        for (String c : REQUIRED) {
            if (isEmpty(r.get(c))) return true;
        }
        return false;
    }

    // Giữ lại nếu sau này cần dùng, hiện tại KHÔNG gọi ở đâu
    private static boolean hasAnyEmpty(Map<String,Object> r) {
        for (String c : COLS) {
            if (isEmpty(r.get(c))) return true;
        }
        return false;
    }

    private static boolean isEmpty(Object v) {
        if (v == null) return true;
        if (v instanceof String s) return s.trim().isEmpty();
        // Với số, chấp nhận 0 là giá trị hợp lệ (không coi là trống)
        return false;
    }

    // Giữ lại nếu sau này muốn bật so sánh "đầy đủ hơn"
    private static double completeness(Map<String,Object> r) {
        int filled = 0;
        for (String c : COLS) {
            if (!isEmpty(r.get(c))) filled++;
        }
        return (double) filled / COLS.length;
    }
    // BƯỚC 3.4: Sao lưu bảng stg_phones vào stg_phones_backup và TRUNCATE stg_phones
    private static void backupAndReplace(Connection conn, List<Map<String,Object>> rows) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS stg_phones_backup LIKE stg_phones");
            st.execute("TRUNCATE TABLE stg_phones_backup");
            st.execute("INSERT INTO stg_phones_backup SELECT * FROM stg_phones");
            // Xóa sạch bảng đích
            st.execute("TRUNCATE TABLE stg_phones");
        }
// BƯỚC 3.5: Ghi đè dữ liệu đã làm sạch vào bảng stg_phones
        String insert = """
            INSERT INTO stg_phones
            (name, screenSize, screenTechnology, screenResolution, camera, chipset, ram, storage, battery, version, color, price, oldPrice, link, rating, numReviews, nfc, releaseDate)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """;

        try (PreparedStatement ps = conn.prepareStatement(insert)) {
            for (Map<String,Object> r : rows) {
                int i = 1;
                ps.setString(i++, str(r.get("name")));
                ps.setString(i++, str(r.get("screenSize")));
                ps.setString(i++, str(r.get("screenTechnology")));
                ps.setString(i++, str(r.get("screenResolution")));
                ps.setString(i++, str(r.get("camera")));
                ps.setString(i++, str(r.get("chipset")));
                ps.setString(i++, str(r.get("ram")));
                ps.setString(i++, str(r.get("storage")));
                setNullableInt(ps, i++, r.get("battery"));
                ps.setString(i++, str(r.get("version")));
                ps.setString(i++, str(r.get("color")));
                setNullableDecimal(ps, i++, r.get("price"));
                setNullableDecimal(ps, i++, r.get("oldPrice"));
                ps.setString(i++, str(r.get("link")));
                setNullableDouble(ps, i++, r.get("rating"));
                setNullableInt(ps, i++, r.get("numReviews"));
                ps.setString(i++, str(r.get("nfc")));
                ps.setString(i++, str(r.get("releaseDate")));
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    // Helpers
    private static String getenvOrThrow(String k) {
        String v = System.getenv(k);
        if (v == null || v.isBlank())
            throw new IllegalArgumentException("Missing env: " + k);
        return v;
    }
    private static String str(Object o) { return o == null ? null : String.valueOf(o); }
    private static double asDouble(Object o) {
        if (o == null) return Double.NaN;
        if (o instanceof Number n) return n.doubleValue();
        try { return Double.parseDouble(o.toString()); } catch (Exception e) { return Double.NaN; }
    }
    private static int asInt(Object o) {
        if (o == null) return Integer.MIN_VALUE;
        if (o instanceof Number n) return n.intValue();
        try { return Integer.parseInt(o.toString()); } catch (Exception e) { return Integer.MIN_VALUE; }
    }
    private static void setNullableInt(PreparedStatement ps, int idx, Object v) throws SQLException {
        if (v == null) ps.setNull(idx, Types.INTEGER);
        else if (v instanceof Number n) ps.setInt(idx, n.intValue());
        else ps.setInt(idx, Integer.parseInt(v.toString()));
    }
    private static void setNullableDouble(PreparedStatement ps, int idx, Object v) throws SQLException {
        if (v == null) ps.setNull(idx, Types.DOUBLE);
        else if (v instanceof Number n) ps.setDouble(idx, n.doubleValue());
        else ps.setDouble(idx, Double.parseDouble(v.toString()));
    }
    private static void setNullableDecimal(PreparedStatement ps, int idx, Object v) throws SQLException {
        if (v == null) ps.setNull(idx, Types.DECIMAL);
        else ps.setBigDecimal(idx, new java.math.BigDecimal(v.toString()));
    }
}