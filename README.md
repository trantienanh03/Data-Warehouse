# Data Warehouse Project

## 📖 Tổng Quan Đề Tài (Project Overview)

Dự án này là một hệ thống **Data Warehouse (Kho dữ liệu)** được xây dựng bằng ngôn ngữ **Java**. Mục tiêu chính của đề tài là mô phỏng và thực hiện quy trình thu thập, lưu trữ và quản lý dữ liệu từ nhiều nguồn khác nhau để phục vụ cho việc phân tích và báo cáo.

Trong kỷ nguyên dữ liệu lớn, việc phân tán dữ liệu ở nhiều nơi (file Excel, CSV, Database rời rạc) gây khó khăn cho việc ra quyết định. Hệ thống này giải quyết vấn đề đó bằng cách tập trung hóa dữ liệu vào một kho chứa duy nhất, nhất quán và đáng tin cậy.

## 🎯 Mục Tiêu Của Dự Án

1.  **Tích hợp dữ liệu (Data Integration):** Thu thập dữ liệu từ các nguồn không đồng nhất (như file .csv, .txt, hoặc database khác).
2.  **Chuẩn hóa dữ liệu:** Làm sạch, chuyển đổi định dạng và xử lý các dữ liệu lỗi trước khi đưa vào kho lưu trữ.
3.  **Lưu trữ lịch sử:** Khác với database thông thường chỉ lưu trạng thái hiện tại, Data Warehouse lưu trữ cả lịch sử thay đổi của dữ liệu theo thời gian.
4.  **Hỗ trợ ra quyết định:** Cung cấp dữ liệu sạch và có cấu trúc để phục vụ các báo cáo thống kê hoặc Business Intelligence (BI).

## ⚙️ Kiến Trúc Hệ Thống (Architecture)

Dự án mô phỏng quy trình chuẩn của một hệ thống Data Warehouse bao gồm các tầng chính:

1.  **Data Sources (Nguồn dữ liệu):**
    *   Nơi dữ liệu thô được sinh ra (ví dụ: file log, danh sách khách hàng, giao dịch hàng ngày).
2.  **Staging Area (Vùng trung gian):**
    *   Dữ liệu thô được đưa vào đây tạm thời để kiểm tra và lọc lỗi. Dữ liệu chưa sạch sẽ không được đi tiếp.
3.  **Data Warehouse (Kho dữ liệu chính):**
    *   Nơi lưu trữ dữ liệu đã được làm sạch, chuẩn hóa và tổ chức theo các chiều (Dimensions) và bảng sự kiện (Fact tables).
4.  **Data Marts (Kho dữ liệu nhỏ - Tùy chọn):**
    *   Các tập con của dữ liệu phục vụ cho từng phòng ban cụ thể (ví dụ: Mart Kinh doanh, Mart Nhân sự).

## 🔄 Quy Trình ETL (Extract - Transform - Load)

Hệ thống tập trung vào xử lý quy trình ETL cốt lõi:
*   **E (Extract):** Trích xuất dữ liệu từ nguồn.
*   **T (Transform):** Biến đổi dữ liệu (xử lý null, định dạng ngày tháng, tính toán các trường dẫn xuất).
*   **L (Load):** Nạp dữ liệu sạch vào Data Warehouse.

## 🛠 Công Nghệ Sử Dụng

*   **Ngôn ngữ lập trình:** Java
*   **Cơ sở dữ liệu:** (MySQL / SQL Server / PostgreSQL - *Tùy thuộc vào cấu hình của bạn*)
*   **Công cụ quản lý:** GitHub để quản lý mã nguồn.

---
