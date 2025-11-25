package org.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class DatabaseInitializer {

    public static void initialize() throws Exception {
        System.out.println("Kiểm tra và khởi tạo Database...");

        String fullUrl = ReaderVariable.getValue("db.warehouse.url");
        String user = ReaderVariable.getValue("db.warehouse.user");
        String pass = ReaderVariable.getValue("db.warehouse.pass");

        // Tách tên DB và URL gốc
        String dbName = fullUrl.substring(fullUrl.lastIndexOf("/") + 1);
        if (dbName.contains("?")) {
            dbName = dbName.substring(0, dbName.indexOf("?"));
        }
        String serverUrl = fullUrl.substring(0, fullUrl.lastIndexOf("/") + 1);

        // 1. Kết nối Server để tạo Database
        try (Connection conn = DriverManager.getConnection(serverUrl, user, pass);
             Statement stmt = conn.createStatement()) {

            String sqlCreateDB = "CREATE DATABASE IF NOT EXISTS " + dbName +
                    " CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci";
            stmt.executeUpdate(sqlCreateDB);
            System.out.println("-> Database '" + dbName + "' đã sẵn sàng.");
        }

        // 2. Kết nối vào DB cụ thể để tạo Bảng
        try (Connection conn = DriverManager.getConnection(fullUrl, user, pass);
             Statement stmt = conn.createStatement()) {

            // --- TẠO BẢNG dim_brand ---
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS dim_brand (" +
                    "brandID INT AUTO_INCREMENT PRIMARY KEY, " +
                    "brandName VARCHAR(100)" + // Sửa theo db_warehouse.sql
                    ")");

            // --- TẠO BẢNG dim_phone_specs ---
            // Đã cập nhật các độ dài VARCHAR theo đúng db_warehouse.sql
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS dim_phone_specs (" +
                    "specID INT AUTO_INCREMENT PRIMARY KEY, " +
                    "screenSize VARCHAR(50), " +
                    "screenTechnology VARCHAR(100), " +
                    "screenResolution VARCHAR(100), " +
                    "camera TEXT, " +
                    "chipset VARCHAR(100), " +
                    "ram VARCHAR(20), " +
                    "storage VARCHAR(20), " +
                    "battery INT, " +
                    "nfc VARCHAR(10)" +
                    ")");

            // --- TẠO BẢNG fact_phones ---
            // QUAN TRỌNG: releaseDate đổi thành TEXT theo đúng file sql của bạn
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS fact_phones (" +
                    "phoneID INT AUTO_INCREMENT PRIMARY KEY, " +
                    "brandID INT, " +
                    "specID INT, " +
                    "productName VARCHAR(255), " +
                    "version VARCHAR(100), " +
                    "color VARCHAR(100), " +
                    "price DECIMAL(15,2), " +
                    "oldPrice DECIMAL(15,2), " +
                    "rating FLOAT, " +
                    "numReviews INT, " +
                    "link TEXT, " +
                    "releaseDate TEXT, " + // <--- ĐÃ SỬA THÀNH TEXT
                    "FOREIGN KEY (brandID) REFERENCES dim_brand(brandID), " +
                    "FOREIGN KEY (specID) REFERENCES dim_phone_specs(specID)" +
                    ")");

            System.out.println("-> Các bảng (Tables) đã sẵn sàng.");
        }
    }
}