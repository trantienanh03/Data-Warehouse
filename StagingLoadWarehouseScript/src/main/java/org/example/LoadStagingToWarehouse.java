package org.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class LoadStagingToWarehouse {

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
            conn.setAutoCommit(false);
            try {
                // 4.5 Tắt kiểm tra khóa ngoại (SET FOREIGN_KEY_CHECKS = 0)
                stmt.executeUpdate("SET FOREIGN_KEY_CHECKS=0");

                // 4.6 TRUNCATE tables (fact_phones, dim_brand, dim_phone_specs)
                stmt.executeUpdate("TRUNCATE TABLE fact_phones");
                stmt.executeUpdate("TRUNCATE TABLE dim_brand");
                stmt.executeUpdate("TRUNCATE TABLE dim_phone_specs");

                // Bật lại khóa ngoại (Bước đệm an toàn, không có trong flow nhưng cần thiết)
                stmt.executeUpdate("SET FOREIGN_KEY_CHECKS=1");

                // 4.7 INSERT dim_brand (SELECT DISTINCT SUBSTRING_INDEX FROM db_staging.stg_phones)
                stmt.executeUpdate(
                        "INSERT INTO dim_brand (brandName) " +
                                "SELECT DISTINCT SUBSTRING_INDEX(name, ' ', 1) " +
                                "FROM db_staging.stg_phones " +
                                "WHERE name IS NOT NULL AND name != ''"
                );

                // 4.8 INSERT dim_phone_specs (SELECT DISTINCT screenSize... FROM db_staging.stg_phones)
                stmt.executeUpdate(
                        "INSERT INTO dim_phone_specs (screenSize, screenTechnology, " +
                                "screenResolution, camera, chipset, ram, storage, battery, nfc) " +
                                "SELECT DISTINCT s.screenSize, s.screenTechnology, s.screenResolution, " +
                                "s.camera, s.chipset, s.ram, s.storage, s.battery, s.nfc " +
                                "FROM db_staging.stg_phones s"
                );

                // 4.9 INSERT fact_phones (JOIN dim_brand, dim_phone_specs FROM db_staging.stg_phones)
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

                conn.commit();
                System.out.println("-> Load Warehouse THÀNH CÔNG.");

            } catch (Exception e) {
                conn.rollback();
                // SET FOREIGN_KEY_CHECKS = 1 (Phục hồi trạng thái nếu lỗi)
                stmt.executeUpdate("SET FOREIGN_KEY_CHECKS=1");
                System.err.println("-> Lỗi khi Load Warehouse, đang rollback...");
                throw e;
            }
        }
    }
}