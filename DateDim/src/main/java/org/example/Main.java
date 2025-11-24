package org.example;

import javax.swing.JFileChooser;
import javax.swing.JOptionPane;
import java.io.File;

public class Main {

    public static void main(String[] args) {
        System.out.println("=== BẮT ĐẦU TÁC VỤ: KHỞI TẠO DATE DIMENSION ===");
        System.out.println("=================================================");

        try {
            // --- BƯỚC 1: HIỂN THỊ CỬA SỔ CHỌN FILE VARIABLE ---
            String configFilePath = selectConfigFile();
            if (configFilePath == null) {
                System.out.println("!!! Người dùng đã hủy chọn file cấu hình. Chương trình dừng lại.");
                return;
            }

            // --- BƯỚC 2: LOAD CẤU HÌNH VỚI ĐƯỜNG DẪN ĐÃ CHỌN ---
            // Yêu cầu class ConfigReader đã được sửa đổi và sẵn có.
            ReaderVariable.loadConfig(configFilePath);
            System.out.println("-> Tải cấu hình từ file: " + configFilePath + " THÀNH CÔNG.");
            System.out.println("----------------------------------------");


            // --- BƯỚC 3: CHẠY TÁC VỤ TẠO DATE DIM ---
            System.out.println("\n[TASK] Bắt đầu: Create Date Dimension...");
            CreateDateDim.runTask();
            System.out.println("[TASK] Hoàn thành: Create Date Dimension.");


            System.out.println("\n=================================================");
            System.out.println("=== TÁC VỤ CREATE DATE DIM HOÀN TẤT THÀNH CÔNG ===");

        } catch (Exception e) {
            System.err.println("\n!!! TÁC VỤ ĐÃ DỪNG LẠI VÌ CÓ LỖI NGHIÊM TRỌNG !!!");
            System.err.println("LỖI CHI TIẾT:");
            e.printStackTrace();
            JOptionPane.showMessageDialog(null, "LỖI: " + e.getMessage(), "Lỗi Nghiêm Trọng", JOptionPane.ERROR_MESSAGE);
        }
    }

    /**
     * Mở hộp thoại để người dùng chọn file variables.csv
     * @return Đường dẫn tuyệt đối của file đã chọn, hoặc null nếu người dùng hủy.
     */
    private static String selectConfigFile() {
        JFileChooser fileChooser = new JFileChooser();
        fileChooser.setDialogTitle("Chọn file variables.csv cho Date Dim");
        fileChooser.setCurrentDirectory(new File(System.getProperty("user.home")));

        int userSelection = fileChooser.showOpenDialog(null);

        if (userSelection == JFileChooser.APPROVE_OPTION) {
            File selectedFile = fileChooser.getSelectedFile();
            return selectedFile.getAbsolutePath();
        }
        return null; // Người dùng hủy
    }
}