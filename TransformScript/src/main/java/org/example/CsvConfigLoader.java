package org.example;

import javax.swing.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CsvConfigLoader {

    // Thêm các biến tĩnh để lưu trữ cấu hình (public để LoadToDataMartSeparateDB có thể truy cập)
    public static String SRC_URL, SRC_USER, SRC_PASS;
    private static boolean configLoaded = false;

    // Sửa đổi: Hàm hiển thị cửa sổ chọn file và tải cấu hình (thành void)
    public static void loadConfigFromCsv() throws Exception {
        if (configLoaded) return;

        // Đảm bảo JFileChooser chạy trong môi trường GUI
        try {
            UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
        } catch (Exception ignore) {}

        JFileChooser fileChooser = new JFileChooser();
        fileChooser.setDialogTitle("Chọn file variable.csv chứa thông tin Database");
        fileChooser.setCurrentDirectory(new File("."));

        int result = fileChooser.showOpenDialog(null);

        if (result == JFileChooser.APPROVE_OPTION) {
            File selectedFile = fileChooser.getSelectedFile();
            System.out.println("Đang đọc cấu hình từ: " + selectedFile.getAbsolutePath());

            // Gọi parseCsv và lưu kết quả vào biến tĩnh
            Properties p = parseCsv(selectedFile);

            // Lưu các thuộc tính vào biến tĩnh (CẬP NHẬT KEY MỚI)
            SRC_URL  = p.getProperty("db.staging.url");
            SRC_USER = p.getProperty("db.staging.user");
            SRC_PASS = p.getProperty("db.staging.pass");
            configLoaded = true;

        } else {
            throw new Exception("Không chọn file cấu hình. Dừng chương trình.");
        }
    }

    // Hàm phân tích cú pháp CSV để lấy các giá trị key/value (GIỮ NGUYÊN Properties return type)
    private static Properties parseCsv(File csvFile) throws IOException, Exception {
        Properties p = new Properties();
        // Regex để tìm: key, value (giả định key không chứa dấu phẩy, value có thể có)
        Pattern pattern = Pattern.compile("^\\s*([^,]+?)\\s*,\\s*\"?([^\"]*)\"?\\s*$");

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                // Bỏ qua dòng trống hoặc dòng chú thích (bắt đầu bằng # hoặc //)
                if (line.trim().isEmpty() || line.trim().startsWith("#") || line.trim().startsWith("//")) {
                    continue;
                }

                Matcher matcher = pattern.matcher(line);
                if (matcher.find()) {
                    String key = matcher.group(1).trim();
                    String value = matcher.group(2).trim();

                    // CẬP NHẬT: Kiểm tra key mới
                    if (key.startsWith("db.staging.")) {
                        p.setProperty(key, value);
                    }
                } else {
                    System.err.println("Cảnh báo: Bỏ qua dòng CSV không đúng định dạng: " + line);
                }
            }
        }

        // Kiểm tra tính đầy đủ của các thuộc tính cần thiết (CẬP NHẬT KEY MỚI)
        String[] requiredProps = {
                "db.staging.url", "db.staging.user", "db.staging.pass",
        };
        for (String prop : requiredProps) {
            if (p.getProperty(prop) == null || p.getProperty(prop).isEmpty()) {
                throw new Exception("Thiếu thuộc tính bắt buộc: " + prop + " trong file variable.csv");
            }
        }

        return p;
    }
}