package org.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class LoadStagingToWarehouse {

    // Hàm đọc config và tạo kết nối đến Database Warehouse
    private static Connection getConnection() throws Exception {
        final String DB_URL = ReaderVariable.getValue("db.warehouse.url");
        final String DB_USER = ReaderVariable.getValue("db.warehouse.user");
        final String DB_PASS = ReaderVariable.getValue("db.warehouse.pass");
        return DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
    }

    public static void runTask() throws Exception {
        System.out.println("Bắt đầu Nhiệm vụ 4: Load Staging to Warehouse...");

        // 4.4 Kết nối với db_warehouse
        try (
                Connection conn = getConnection();
                Statement stmt = conn.createStatement()
        ) {
            // Tắt chế độ tự động lưu (AutoCommit) để quản lý Transaction.
            // Giúp đảm bảo dữ liệu toàn vẹn: hoặc là nạp thành công tất cả, hoặc là không nạp gì cả (nếu lỗi).
            conn.setAutoCommit(false);

            try {
                // 4.5 Tắt kiểm tra khóa ngoại (SET FOREIGN_KEY_CHECKS = 0)
                // Cần tắt tạm thời để có thể TRUNCATE bảng cha mà không bị lỗi ràng buộc dữ liệu.
                stmt.executeUpdate("SET FOREIGN_KEY_CHECKS=0");

                // 4.6 TRUNCATE tables (fact_phones, dim_brand, dim_phone_specs)
                // Xóa sạch dữ liệu cũ trong Warehouse trước khi nạp dữ liệu mới từ Staging.
                stmt.executeUpdate("TRUNCATE TABLE fact_phones");
                stmt.executeUpdate("TRUNCATE TABLE dim_brand");
                stmt.executeUpdate("TRUNCATE TABLE dim_phone_specs");

                // Bật lại khóa ngoại ngay sau khi xóa xong để đảm bảo an toàn cho các bước Insert sau
                stmt.executeUpdate("SET FOREIGN_KEY_CHECKS=1");

                // 4.7 INSERT dim_brand
                // Lấy danh sách thương hiệu duy nhất từ Staging (cắt chuỗi lấy từ đầu tiên làm tên hãng).
                stmt.executeUpdate(
                        "INSERT INTO dim_brand (brandName) " +
                                "SELECT DISTINCT SUBSTRING_INDEX(name, ' ', 1) " +
                                "FROM db_staging.stg_phones " +
                                "WHERE name IS NOT NULL AND name != ''"
                );

                // 4.8 INSERT dim_phone_specs
                // Lấy danh sách các thông số kỹ thuật duy nhất để nạp vào bảng Dimension.
                stmt.executeUpdate(
                        "INSERT INTO dim_phone_specs (screenSize, screenTechnology, " +
                                "screenResolution, camera, chipset, ram, storage, battery, nfc) " +
                                "SELECT DISTINCT s.screenSize, s.screenTechnology, s.screenResolution, " +
                                "s.camera, s.chipset, s.ram, s.storage, s.battery, s.nfc " +
                                "FROM db_staging.stg_phones s"
                );

                // 4.9 INSERT fact_phones
                // Thực hiện JOIN dữ liệu từ Staging với các bảng Dimension vừa tạo để lấy ID (Key).
                // Sử dụng toán tử <=> để so sánh an toàn với giá trị NULL.
                stmt.executeUpdate(
                        "INSERT INTO fact_phones (brandID, specID, productName, version, " +
                                "color, price, oldPrice, rating, numReviews, link, releaseDate) " +
                                "SELECT b.brandID, sp.specID, s.name, s.version, s.color, s.price, " +
                                "s.oldPrice, s.rating, s.numReviews, s.link, s.releaseDate " +
                                "FROM db_staging.stg_phones s " +
                                "JOIN dim_brand b ON b.brandName = SUBSTRING_INDEX(s.name, ' ', 1) " +
                                "JOIN dim_phone_specs sp ON " +
                                "    sp.screenSize <=> s.screenSize AND " +
                                "    sp.screenTechnology <=> s.screenTechnology AND " +
                                "    sp.screenResolution <=> s.screenResolution AND " +
                                "    sp.camera <=> s.camera AND " +
                                "    sp.chipset <=> s.chipset AND " +
                                "    sp.ram <=> s.ram AND " +
                                "    sp.storage <=> s.storage AND " +
                                "    sp.battery <=> s.battery AND " +
                                "    sp.nfc <=> s.nfc"
                );

                // Xác nhận giao dịch thành công, lưu dữ liệu vĩnh viễn
                conn.commit();
                System.out.println("-> Load Warehouse THÀNH CÔNG.");

            } catch (Exception e) {
                // Nếu có lỗi, Rollback toàn bộ các lệnh trên để trả lại trạng thái sạch sẽ cho DB
                conn.rollback();
                stmt.executeUpdate("SET FOREIGN_KEY_CHECKS=1");
                System.err.println("-> Lỗi khi Load Warehouse, đang rollback...");
                throw e;
            }
        }
    }
}