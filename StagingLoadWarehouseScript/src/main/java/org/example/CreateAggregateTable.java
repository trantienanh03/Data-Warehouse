package org.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class CreateAggregateTable {

    private static Connection getConnection() throws Exception {
        final String DB_URL = ReaderVariable.getValue("db.warehouse.url");
        final String DB_USER = ReaderVariable.getValue("db.warehouse.user");
        final String DB_PASS = ReaderVariable.getValue("db.warehouse.pass");
        return DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
    }

    public static void runTask() throws Exception {
        System.out.println("Bắt đầu Nhiệm vụ 5: Create Aggregate Table...");

        // Định nghĩa các câu lệnh SQL
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
                Connection conn = getConnection();
                Statement stmt = conn.createStatement()
        ) {
            stmt.executeUpdate("SET FOREIGN_KEY_CHECKS=0");

            // 5.1 CREATE TABLE IF NOT EXISTS (Tạo mới table agg_brand_summary)
            stmt.executeUpdate(sqlCreateAggTable);

            // 5.2 TRUNCATE agg_brand_summary (xoá dữ liệu cũ)
            stmt.executeUpdate(sqlTruncateAgg);

            // 5.3 INSERT AGGREGATED DATA (Select AVG(price), sum(numReviews)... FROM fact_phones JOIN dim_brand)
            stmt.executeUpdate(sqlInsertAgg);

            stmt.executeUpdate("SET FOREIGN_KEY_CHECKS=1");

            System.out.println("-> Create Aggregate THÀNH CÔNG.");
        }
    }
}