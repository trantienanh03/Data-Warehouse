package org.example;

import javax.swing.JFileChooser;
import javax.swing.JOptionPane;
import java.io.File;

public class Main {

    public static void main(String[] args) {
        System.out.println("=== BẮT ĐẦU QUY TRÌNH ETL TỔNG THỂ ===");
        System.out.println("========================================");

        try {
            // --- BƯỚC 1: HIỂN THỊ CỬA SỔ CHỌN FILE VARIABLE ---
            String configFilePath = selectConfigFile();
            if (configFilePath == null) {
                // Người dùng hủy chọn file
                System.out.println("!!! Người dùng đã hủy chọn file cấu hình. Chương trình dừng lại.");
                return;
            }

            // --- BƯỚC 2: LOAD CẤU HÌNH VỚI ĐƯỜNG DẪN ĐÃ CHỌN ---
            ReaderVariable.loadConfig(configFilePath);
            System.out.println("-> Tải cấu hình từ file: " + configFilePath + " THÀNH CÔNG.");
            System.out.println("----------------------------------------");


            // Chạy tác vụ 1
            System.out.println("\n[TASK 1/2] Bắt đầu: Load Staging to Warehouse...");
            LoadStagingToWarehouse.runTask();
            System.out.println("[TASK 1/2] Hoàn thành: Load Staging to Warehouse.");

            System.out.println("----------------------------------------");

            // Chạy tác vụ 2
            System.out.println("[TASK 2/2] Bắt đầu: Create Aggregate Table...");
            CreateAggregateTable.runTask();
            System.out.println("[TASK 2/2] Hoàn thành: Create Aggregate Table.");

            System.out.println("\n========================================");
            System.out.println("=== QUY TRÌNH ETL TỔNG THỂ HOÀN TẤT THÀNH CÔNG ===");

        } catch (Exception e) {
            System.err.println("\n!!! QUY TRÌNH ETL ĐÃ DỪNG LẠI VÌ CÓ LỖI NGHIÊM TRỌNG !!!");
            System.err.println("LỖI CHI TIẾT:");
            e.printStackTrace();
            JOptionPane.showMessageDialog(null, "LỖI ETL: " + e.getMessage(), "Lỗi Nghiêm Trọng", JOptionPane.ERROR_MESSAGE);
        }
    }

    /**
     * Mở hộp thoại để người dùng chọn file variables.csv
     * @return Đường dẫn tuyệt đối của file đã chọn, hoặc null nếu người dùng hủy.
     */
    private static String selectConfigFile() {
        JFileChooser fileChooser = new JFileChooser();
        fileChooser.setDialogTitle("Chọn file variables.csv");
        // Đặt thư mục mặc định là thư mục người dùng
        fileChooser.setCurrentDirectory(new File(System.getProperty("user.home")));

        int userSelection = fileChooser.showOpenDialog(null);

        if (userSelection == JFileChooser.APPROVE_OPTION) {
            File selectedFile = fileChooser.getSelectedFile();
            return selectedFile.getAbsolutePath();
        }
        return null; // Người dùng hủy
    }
}