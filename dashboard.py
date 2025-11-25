import streamlit as st
import pandas as pd
import mysql.connector

# 1. CẤU HÌNH KẾT NỐI DATABASE (Ông sửa lại user/pass cho đúng máy ông nhé)
def get_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",      # Sửa lại user của ông
        password="",      # Sửa lại pass của ông
        database="db_warehouse" # Kết nối vào kho dữ liệu
    )

# 2. HÀM LOAD DỮ LIỆU TỪ BẢNG AGGREGATE
def load_data():
    conn = get_connection()
    # Lấy dữ liệu từ bảng tổng hợp ông đã tạo ở bước 5
    query = """
    SELECT 
        brandName, 
        averagePrice, 
        totalReviews, 
        averageRating, 
        phoneCount 
    FROM agg_brand_summary
    ORDER BY averagePrice DESC
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# 3. GIAO DIỆN WEB (STREAMLIT)
st.set_page_config(page_title="CellphoneS Analytics", layout="wide")

st.title("📊 Dashboard Phân Tích Thị Trường Điện Thoại")
st.markdown("Báo cáo tổng hợp dữ liệu từ Data Warehouse")

# Load data
try:
    df = load_data()
    
    # --- PHẦN 1: KPI TỔNG QUAN ---
    col1, col2, col3 = st.columns(3)
    col1.metric("Tổng số Hãng", len(df))
    col2.metric("Tổng mẫu điện thoại", df['phoneCount'].sum())
    col3.metric("Tổng lượt đánh giá", f"{df['totalReviews'].sum():,}")

    st.divider()

    # --- PHẦN 2: BIỂU ĐỒ ---
    
    # Cột 1: Biểu đồ giá trung bình
    c1, c2 = st.columns(2)
    
    with c1:
        st.subheader("💰 Giá trung bình theo Hãng (VNĐ)")
        # Vẽ biểu đồ cột
        st.bar_chart(df.set_index("brandName")["averagePrice"])

    with c2:
        st.subheader("⭐ Độ quan tâm (Tổng Reviews)")
        # Vẽ biểu đồ tròn/cột cho reviews
        st.bar_chart(df.set_index("brandName")["totalReviews"], color="#ffaa00")

    # --- PHẦN 3: SO SÁNH RATING ---
    st.subheader("📈 Chất lượng sản phẩm (Rating trung bình)")
    st.line_chart(df.set_index("brandName")["averageRating"])

    # --- PHẦN 4: DỮ LIỆU CHI TIẾT ---
    st.subheader("📋 Dữ liệu chi tiết từ Mart")
    st.dataframe(df, use_container_width=True)

except Exception as e:
    st.error(f"Lỗi kết nối Database: {e}")
    st.info("Ông nhớ check lại user/pass trong hàm get_connection() nhé!")