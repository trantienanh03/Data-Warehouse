https://docs.google.com/document/d/1pS68V4TiHv1pAckOT17aJEeJtPxjFawHMldX50_ikCQ/edit?tab=t.0#heading=h.3e4ggr9iis6f
# HƯỚNG DẪN SỬ DỤNG - Load CSV to Database Staging
## 📋 Mô tả
Chương trình Java để load dữ liệu từ file CSV vào bảng staging `stg_phones` trong MySQL database.

### 2. Thư viện dependencies
```
```xml
<dependency>
    <groupId>com.mysql</groupId>
    <artifactId>mysql-connector-j</artifactId>
    <version>8.3.0</version>
</dependency>

<dependency>
    <groupId>com.opencsv</groupId>
    <artifactId>opencsv</artifactId>
    <version>5.9</version>
</dependency>
```

---

## Cấu trúc dự án

```
LoadToDbStaging/
├── pom.xml
├── config.csv                            # File cấu hình (MỚI - thay thế application.properties)
├── src/
│   ├── main/
│   │   ├── java/org/example/
│   │   │   ├── LoadCSVToStaging.java    # Class chính load CSV
│   │   │   └── fix_products.csv          # File dữ liệu CSV
│   │   └── resources/
│   └── test/
└── target/
```

---

##  Cấu hình

### 1. File `config.csv` (Thay thế application.properties)
Đường dẫn: `config.csv` (thư mục gốc dự án)

```csv
key,value
db.url,jdbc:mysql://localhost:3306/db_staging?useUnicode=true&characterEncoding=UTF-8
db.user,root
db.pass,
csv.file,src/main/java/org/example/fix_products.csv
```

**Lưu ý:** 
- File config.csv phải đặt ở thư mục gốc của dự án (cùng cấp với pom.xml)
- Nếu password của database rỗng, để trống cột value như trên
- Nếu có password, điền vào dòng db.pass

### 2. Cấu hình Database
Tạo database và bảng trong MySQL:

```sql
-- Tạo database
CREATE DATABASE db_staging 
USE db_staging;

-- Tạo bảng stg_phones
CREATE TABLE stg_phones (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255),
    screenSize VARCHAR(50),
    screenTechnology VARCHAR(100),
    screenResolution VARCHAR(100),
    camera VARCHAR(255),
    chipset VARCHAR(100),
    ram VARCHAR(50),
    storage VARCHAR(50),
    battery INT,
    version VARCHAR(50),
    color VARCHAR(50),
    price DECIMAL(15,2),
    oldPrice DECIMAL(15,2),
    link VARCHAR(500),
    rating FLOAT,
    numReviews INT,
    nfc VARCHAR(10),
    releaseDate VARCHAR(50),
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

