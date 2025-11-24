package org.example;

import java.io.BufferedReader;
import java.io.FileReader; // Dùng để đọc file từ đường dẫn hệ thống
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ReaderVariable {
    // Xóa biến CONFIG_FILE và static { ... }
    private static Map<String, String> configMap = new HashMap<>();

    // Thêm phương thức để load cấu hình từ đường dẫn file
    public static void loadConfig(String filePath) throws Exception {
        configMap.clear(); // Xóa cấu hình cũ (nếu có)

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {

            String line;
            // Bỏ qua dòng tiêu đề (variable,value)
            reader.readLine();

            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty() || line.trim().startsWith("#")) {
                    continue;
                }

                String[] parts = line.split(",", 2);
                if (parts.length == 2) {
                    String key = parts[0].trim();
                    // Xóa """ ở đầu và cuối (nếu có)
                    String value = parts[1].trim().replaceAll("^\"\"\"|\"\"\"$", "");
                    configMap.put(key, value);
                }
            }
        } catch (IOException e) {
            throw new Exception("Lỗi khi đọc file cấu hình: " + filePath, e);
        }
    }

    // Phương thức tĩnh để lấy giá trị cấu hình (giữ nguyên)
    public static String getValue(String key) throws Exception {
        String value = configMap.get(key);
        if (value == null) {
            throw new Exception("Thiếu khóa cấu hình: " + key + ". Vui lòng kiểm tra lại file variables.");
        }
        return value;
    }
}
