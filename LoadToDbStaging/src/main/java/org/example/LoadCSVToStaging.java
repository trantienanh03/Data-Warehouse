package org.example;

import com.opencsv.CSVReader;

import javax.swing.*;
import javax.swing.filechooser.FileNameExtensionFilter;
import java.io.File;
import java.io.FileReader;
import java.sql.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class LoadCSVToStaging {

    private static String URL;
    private static String USER;
    private static String PASS;
    private static String CSV_FILE;

    public static void main(String[] args) {
        // Đọc cấu hình từ file properties
        String configPath = chooseVariableCsv();
        loadConfig(configPath);


        String sql = """
            INSERT INTO stg_phones (
                name, screenSize, screenTechnology, screenResolution, camera,
                chipset, ram, storage, battery, version, color, price, oldPrice,
                link, rating, numReviews, nfc, releaseDate
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """;

        try (
                Connection conn = DriverManager.getConnection(URL, USER, PASS);
                PreparedStatement ps = conn.prepareStatement(sql);
                CSVReader reader = new CSVReader(new FileReader(CSV_FILE))
        ) {
            String[] row;
            boolean skipHeader = true;
            int successCount = 0;
            int errorCount = 0;

            while ((row = reader.readNext()) != null) {
                if (skipHeader) { skipHeader = false; continue; } // bỏ dòng header

                try {
                    System.out.println("Loading: " + Arrays.toString(row));

                    ps.setString(1, truncate(row[0], 255));   // name
                    ps.setString(2, truncate(row[1], 50));    // screenSize
                    ps.setString(3, truncate(row[2], 100));   // screenTechnology
                    ps.setString(4, truncate(row[3], 100));   // screenResolution
                    ps.setString(5, truncate(row[4], 255));   // camera
                    ps.setString(6, truncate(row[5], 100));   // chipset
                    ps.setString(7, truncate(row[6], 50));    // ram
                    ps.setString(8, truncate(row[7], 50));    // storage

                    // battery: chuyển về int, nếu rỗng → NULL
                    if (row[8] == null || row[8].isEmpty()) ps.setNull(9, Types.INTEGER);
                    else ps.setInt(9, Integer.parseInt(row[8]));

                    ps.setString(10, truncate(row[9], 50));   // version (giới hạn 50 ký tự)
                    ps.setString(11, truncate(row[10], 50));  // color

                    // price
                    if (row[11] == null || row[11].isEmpty()) ps.setNull(12, Types.DECIMAL);
                    else ps.setBigDecimal(12, new java.math.BigDecimal(row[11].replaceAll("\\D", "")));

                    // oldPrice
                    if (row[12] == null || row[12].isEmpty()) ps.setNull(13, Types.DECIMAL);
                    else ps.setBigDecimal(13, new java.math.BigDecimal(row[12].replaceAll("\\D", "")));

                    ps.setString(14, truncate(row[13], 500));  // link

                    // rating
                    if (row[14] == null || row[14].isEmpty()) ps.setNull(15, Types.FLOAT);
                    else ps.setFloat(15, Float.parseFloat(row[14]));

                    // numReviews
                    if (row[15] == null || row[15].isEmpty()) ps.setNull(16, Types.INTEGER);
                    else ps.setInt(16, Integer.parseInt(row[15]));

                    ps.setString(17, truncate(row[16], 10));  // nfc
                    ps.setString(18, truncate(row[17], 50));  // releaseDate

                    ps.executeUpdate();
                    successCount++;

                } catch (Exception rowError) {
                    errorCount++;
                    System.err.println("Lỗi dòng: " + Arrays.toString(row));
                    System.err.println("Chi tiết: " + rowError.getMessage());
                }
            }

            System.out.println("\nDONE: Load thành công " + successCount + " dòng");
            if (errorCount > 0) {
                System.out.println("Có " + errorCount + " dòng lỗi bị bỏ qua");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Hàm cắt chuỗi để tránh lỗi Data too long
    private static String truncate(String value, int maxLength) {
        if (value == null || value.isEmpty()) return value;
        return value.length() > maxLength ? value.substring(0, maxLength) : value;
    }

    // Hàm đọc cấu hình từ file variable.csv
    private static void loadConfig(String configFilePath) {
        try (CSVReader reader = new CSVReader(new FileReader(configFilePath))) {
            String[] row;
            boolean skipHeader = true;
            Map<String, String> variables = new HashMap<>();

            while ((row = reader.readNext()) != null) {
                if (skipHeader) {
                    skipHeader = false;
                    continue;
                }

                if (row.length >= 2) {
                    String key = row[0].trim();
                    String value = row[1].trim();
                    if (value.startsWith("\"") && value.endsWith("\"")) {
                        value = value.substring(1, value.length() - 1);
                    }
                    variables.put(key, value);
                }
            }

            URL = variables.get("db.url");
            USER = variables.get("db.user");
            PASS = variables.get("db.pass");
            CSV_FILE = variables.get("csv.file");

            System.out.println("Đã load cấu hình từ variable.csv");

        } catch (Exception e) {
            System.err.println("Lỗi khi đọc variable.csv: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    private static String chooseVariableCsv() {
        try {
            // Dùng look and feel của hệ điều hành
            UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
        } catch (Exception ignored) {}

        JFileChooser fileChooser = new JFileChooser();
        fileChooser.setDialogTitle("Chọn file variable.csv");
        fileChooser.setFileFilter(new FileNameExtensionFilter("CSV files", "csv"));

        int result = fileChooser.showOpenDialog(null);
        if (result == JFileChooser.APPROVE_OPTION) {
            File selectedFile = fileChooser.getSelectedFile();
            System.out.println("Đã chọn file: " + selectedFile.getAbsolutePath());
            return selectedFile.getAbsolutePath();
        } else {
            System.err.println("Không chọn file cấu hình nào. Dừng chương trình.");
            System.exit(1);
            return null;
        }
    }
}
