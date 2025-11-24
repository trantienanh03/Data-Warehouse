package org.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;

import org.joda.time.LocalDate;

public class CreateDateDim {

    // Xóa các biến cấu hình cũ và phương thức loadConfig()

    private static Connection getWarehouseConnection() throws Exception {
        // 🚨 Lấy cấu hình DB WAREHOUSE từ ConfigReader đã được Main.java nạp
        final String DB_URL = ReaderVariable.getValue("db.warehouse.url");
        final String DB_USER = ReaderVariable.getValue("db.warehouse.user");
        final String DB_PASS = ReaderVariable.getValue("db.warehouse.pass");

        // Trả về kết nối
        return DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
    }
    // ===================================


    // Đổi tên từ main() sang runTask() và ném lỗi ra ngoài
    public static void runTask() throws Exception {
        System.out.println("Bắt đầu Nhiệm vụ 6: Create Date Dimension...");

        try (
                // Kết nối vào db_warehouse
                Connection conn = getWarehouseConnection();
                Statement stmt = conn.createStatement()
        ) {

            // 1. Tạo cấu trúc bảng dim_date
            String sqlCreateTable = "CREATE TABLE IF NOT EXISTS dim_date (" +
                    "  date_key INT PRIMARY KEY," +
                    "  full_date DATE," +
                    "  day_of_week VARCHAR(10)," +
                    "  day_of_month INT," +
                    "  day_of_year INT," +
                    "  month_name VARCHAR(10)," +
                    "  month_of_year INT," +
                    "  quarter_of_year VARCHAR(2)," +
                    "  year INT" +
                    ")";
            stmt.executeUpdate(sqlCreateTable);

            // 2. Xóa dữ liệu cũ
            stmt.executeUpdate("TRUNCATE TABLE dim_date");

            // 3. Chuẩn bị câu lệnh INSERT
            String sqlInsert = "INSERT INTO dim_date (date_key, full_date, day_of_week, day_of_month, " +
                    "day_of_year, month_name, month_of_year, quarter_of_year, year) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

            PreparedStatement pstmt = conn.prepareStatement(sqlInsert);

            // Logic chính: Tạo lịch vạn niên từ 2015 đến 2025
            LocalDate startDate = new LocalDate(2015, 1, 1);
            LocalDate endDate = new LocalDate(2025, 12, 31);

            System.out.println("-> Đang tạo dữ liệu lịch từ " + startDate + " đến " + endDate);

            int count = 0;
            for (LocalDate date = startDate; date.isBefore(endDate.plusDays(1)); date = date.plusDays(1)) {

                String dayOfWeek = date.dayOfWeek().getAsText();
                String monthName = date.monthOfYear().getAsText();
                int quarter = (date.getMonthOfYear() - 1) / 3 + 1;
                String quarterName = "Q" + quarter;

                // Gán giá trị vào câu lệnh INSERT
                pstmt.setInt(1, Integer.parseInt(date.toString("yyyyMMdd"))); // date_key (vd: 20251105)
                pstmt.setDate(2, new java.sql.Date(date.toDate().getTime())); // full_date
                pstmt.setString(3, dayOfWeek); // day_of_week
                pstmt.setInt(4, date.getDayOfMonth()); // day_of_month
                pstmt.setInt(5, date.getDayOfYear()); // day_of_year
                pstmt.setString(6, monthName); // month_name
                pstmt.setInt(7, date.getMonthOfYear()); // month_of_year
                pstmt.setString(8, quarterName); // quarter_of_year
                pstmt.setInt(9, date.getYear()); // year

                pstmt.addBatch(); // Thêm vào lô
                count++;
            }

            // 4. Thực thi
            pstmt.executeBatch();

            System.out.println("-> Create Date Dimension THÀNH CÔNG. Đã chèn " + count + " ngày vào db_warehouse.");
        }
        // Lỗi sẽ được ném (throw) ra ngoài để Main.java xử lý
    }
}