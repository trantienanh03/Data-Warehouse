package org.example;

import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class CreateAggregateTable {

    // Xóa toàn bộ phần loadConfig() và các biến tĩnh cũ

    private static Connection getConnection() throws Exception {
        // Lấy cấu hình DB WAREHOUSE từ ConfigReader
        final String DB_URL = ReaderVariable.getValue("db.warehouse.url");
        final String DB_USER = ReaderVariable.getValue("db.warehouse.user");
        final String DB_PASS = ReaderVariable.getValue("db.warehouse.pass");

        return DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
    }
    // ===================================

    public static void runTask() throws Exception {
        System.out.println("Bắt đầu Nhiệm vụ 5: Create Aggregate Table...");

        // SQL (Giữ nguyên)
        String sqlCreateAggTable = "CREATE TABLE IF NOT EXISTS agg_brand_summary (" +
                "brandID INT PRIMARY KEY, " +
                "brandName VARCHAR(100), " +
                "averagePrice DECIMAL(15, 2), " +
                "totalReviews INT, " +
                "averageRating FLOAT, " +
                "phoneCount INT)";
        String sqlTruncateAgg = "TRUNCATE TABLE agg_brand_summary";
        String sqlInsertAgg = "INSERT INTO agg_brand_summary (brandID, brandName, averagePrice, " +
                "totalReviews, averageRating, phoneCount) " +
                "SELECT " +
                "    b.brandID, " +
                "    b.brandName, " +
                "    AVG(f.price) AS averagePrice, " +
                "    SUM(f.numReviews) AS totalReviews, " +
                "    AVG(f.rating) AS averageRating, " +
                "    COUNT(f.phoneID) AS phoneCount " +
                "FROM fact_phones f " +
                "JOIN dim_brand b ON f.brandID = b.brandID " +
                "GROUP BY b.brandID, b.brandName";

        try (
                Connection conn = getConnection(); // Kết nối tới DB Warehouse
                Statement stmt = conn.createStatement()
        ) {
            // Tắt khóa ngoại (để TRUNCATE)
            stmt.executeUpdate("SET FOREIGN_KEY_CHECKS=0");

            // 1. Đảm bảo bảng tồn tại
            stmt.executeUpdate(sqlCreateAggTable);
            // 2. Dọn dẹp bảng
            stmt.executeUpdate(sqlTruncateAgg);
            // 3. Tải dữ liệu tổng hợp
            stmt.executeUpdate(sqlInsertAgg);

            // Bật lại khóa ngoại
            stmt.executeUpdate("SET FOREIGN_KEY_CHECKS=1");

            System.out.println("-> Create Aggregate THÀNH CÔNG.");
        }
    }
}