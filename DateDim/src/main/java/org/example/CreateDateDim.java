package org.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import org.joda.time.LocalDate;

public class CreateDateDim {

    private static Connection getWarehouseConnection() throws Exception {
        final String DB_URL = ReaderVariable.getValue("db.warehouse.url");
        final String DB_USER = ReaderVariable.getValue("db.warehouse.user");
        final String DB_PASS = ReaderVariable.getValue("db.warehouse.pass");
        return DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
    }

    public static void runTask() throws Exception {
        System.out.println("Bắt đầu Nhiệm vụ 6: Create Date Dimension...");

        // 6.3 Kết nối với db_warehouse
        try (
                Connection conn = getWarehouseConnection();
                Statement stmt = conn.createStatement()
        ) {

            // 6.4 CREATE TABLE IF NOT EXISTS (tạo table dim_date với các thuộc tính date_key, full_date...)
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

            // 6.5 TRUNCATE dim_date (xoá dữ liệu cũ nếu có)
            stmt.executeUpdate("TRUNCATE TABLE dim_date");

            String sqlInsert = "INSERT INTO dim_date (date_key, full_date, day_of_week, day_of_month, " +
                    "day_of_year, month_name, month_of_year, quarter_of_year, year) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

            PreparedStatement pstmt = conn.prepareStatement(sqlInsert);

            // 6.6 Generate Calendar Data (Joda-Time: LocalDate Từ 01-01-2015 đến 31-12-2025 Loop: date.plusDays(1))
            LocalDate startDate = new LocalDate(2015, 1, 1);
            LocalDate endDate = new LocalDate(2025, 12, 31);
            System.out.println("-> Đang tạo dữ liệu lịch từ " + startDate + " đến " + endDate);

            int count = 0;
            for (LocalDate date = startDate; date.isBefore(endDate.plusDays(1)); date = date.plusDays(1)) {

                // 6.7 Extract Data Attributes (Cho mỗi ngày: date_key, quarter...)
                String dayOfWeek = date.dayOfWeek().getAsText();
                String monthName = date.monthOfYear().getAsText();
                int quarter = (date.getMonthOfYear() - 1) / 3 + 1;
                String quarterName = "Q" + quarter;

                pstmt.setInt(1, Integer.parseInt(date.toString("yyyyMMdd")));
                pstmt.setDate(2, new java.sql.Date(date.toDate().getTime()));
                pstmt.setString(3, dayOfWeek);
                pstmt.setInt(4, date.getDayOfMonth());
                pstmt.setInt(5, date.getDayOfYear());
                pstmt.setString(6, monthName);
                pstmt.setInt(7, date.getMonthOfYear());
                pstmt.setString(8, quarterName);
                pstmt.setInt(9, date.getYear());

                pstmt.addBatch();
                count++;
            }

            // 6.8 Batch Insert (pstmt.addBatch() -> executeBatch())
            pstmt.executeBatch();

            System.out.println("-> Create Date Dimension THÀNH CÔNG. Đã chèn " + count + " ngày vào db_warehouse.");
        }
    }
}