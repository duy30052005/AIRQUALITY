# 🌍 Hanoi Air Quality: Real-time Data Warehouse & AI Forecasting

## 📝 Giới thiệu
Dự án là một hệ thống AI End-to-End thực hiện việc giám sát, phân tích và dự báo chất lượng không khí tại Hà Nội. Hệ thống tự động thu thập dữ liệu thời gian thực (Real-time ETL), lưu trữ tối ưu trên Cloud Data Warehouse (Supabase) và áp dụng thuật toán Học máy (Machine Learning) tiên tiến để dự báo mức độ ô nhiễm trong tương lai, hỗ trợ đưa ra cảnh báo sớm.

## 🚀 Công nghệ và Kỹ thuật cốt lõi
* **Data Engineering (ETL Pipeline):** Python (Pandas, SQLAlchemy), Flask, APScheduler.
* **Database:** PostgreSQL (Supabase) tối ưu hóa Connection Pool cho môi trường Cloud.
* **Machine Learning / AI:** **Gradient Boosting Regressor (GBR)** dự báo nồng độ khí dựa trên chuỗi thời gian và sự tương tác phức tạp của thời tiết.
* **Feature Engineering:** Kỹ thuật trễ (Lags), Trung bình trượt (Rolling Mean), và Mã hóa chu kỳ thời gian (Cyclical Encoding với Sin/Cos).
* **Visualization:** Matplotlib nâng cao (hiển thị dữ liệu thực tế, dữ liệu nội suy và dự báo tương lai).

## 🗄️ Kiến trúc Dữ liệu (Data Architecture)
Hệ thống lưu trữ được thiết kế theo mô hình **Galaxy Schema (Lược đồ chòm sao)**, cho phép tích hợp đa nguồn dữ liệu và tối ưu hóa tốc độ truy vấn phân tích đa chiều (OLAP). 


![Galaxy Schema Diagram](<img width="895" height="522" alt="image" src="https://github.com/user-attachments/assets/b78f11a2-1da0-4a6a-b586-e97daae3dd9a" />

)

## 💡 Các tính năng nổi bật

### 1. Luồng dữ liệu tự động (Real-Time ETL Pipeline)
* **Trích xuất (Extract):** Tự động gọi API từ **OpenAQ** (Dữ liệu không khí) và **Open-Meteo** (Dữ liệu thời tiết tương lai) mỗi 15 - 60 phút.
* **Biến đổi (Transform):** Xử lý múi giờ (Asia/Bangkok), chuẩn hóa kiểu dữ liệu và ánh xạ thông minh các Dimension keys.
* **Nạp (Load):** Cơ chế "Smart Check" và `DELETE -> INSERT` giúp ngăn chặn tuyệt đối tình trạng lặp dữ liệu (Duplication) khi chạy trên server liên tục.

### 2. Kỹ thuật Feature Engineering chuyên sâu
Mô hình Học máy không chỉ sử dụng dữ liệu thô mà được huấn luyện trên tập đặc trưng (Features) đã qua xử lý phức tạp:
* Tính toán chỉ số đọng khí (Stagnation Index) và điểm sương (Dew Point).
* Áp dụng mã hóa lượng giác (Sine/Cosine transformation) cho Giờ và Ngày trong tuần để mô hình hiểu được tính chu kỳ tuần hoàn của thời gian.

### 3. Hệ thống Dự báo bằng Gradient Boosting
* Huấn luyện mô hình Gradient Boosting Regressor với hàm mất mát Huber (chống nhiễu ngoại lai) để đưa ra dự báo.
* Kết hợp dữ liệu thời tiết tương lai (Nhiệt độ, Độ ẩm, Gió) để dự báo biến động nồng độ PM2.5, PM10, CO, NO2, SO2, O3.
* Xử lý mượt mà các khoảng trống dữ liệu bằng nội suy tuyến tính, giới hạn trần/sàn (Cap/Floor) để đảm bảo kết quả logic và lưu trực tiếp vào Database.

## 📂 Cấu trúc Repository
* `AirQualityRealTime.py`: Khối Web Server (Flask) & Scheduler quản lý toàn bộ luồng ETL tự động trên Production (Render).
* `GBRmodel.ipynb`: Notebook chứa mã nguồn huấn luyện mô hình GBR, xử lý Feature Engineering và dự báo.
* `ETL_real_time.ipynb`: Notebook thực hiện quy trình ETL thủ công và render giao diện Dashboard trực quan.
* `requirements.txt`: Danh sách các thư viện môi trường cần thiết.

## 📊 Giao diện & Kết quả

| Dashboard Dự báo (GBR & Matplotlib) | Biểu đồ Huấn luyện & Đánh giá |
|:---:|:---:|
| ![Dashboard](https://github.com/user-attachments/assets/62bd4ba6-9571-4cd6-b45f-83b444c804a4) | ![Biểu đồ](https://github.com/user-attachments/assets/a0a18d6f-5826-47c2-9de5-2251f20f8c22) |

---

## 🛠 Hướng dẫn Triển khai (Local Setup)

### Bước 1: Cài đặt môi trường
Clone repository và cài đặt các thư viện phụ thuộc:
```bash
git clone [https://github.com/duy30052005/AIRQUALITY.git](https://github.com/duy30052005/AIRQUALITY.git)
cd AIRQUALITY
pip install -r requirements.txt
```

### Bước 2: Cấu hình biến môi trường
Tạo file `.env` tại thư mục gốc và khai báo các thông tin bảo mật (Hệ thống sẽ tự nhận diện qua `os.getenv`):
```env
DB_PASSWORD=mật_khẩu_supabase_của_bạn
OPENAQ_API_KEY=api_key_của_bạn
PORT=5000
```
*Lưu ý: Không commit file `.env` lên Github.*

### Bước 3: Khởi chạy ETL Server
Khởi động hệ thống thu thập dữ liệu ngầm và Web Server:
```bash
python AirQualityRealTime.py
```
*Hệ thống sẽ ghi log chi tiết quá trình kiểm tra API và nạp dữ liệu vào Database.*

### Bước 4: Chạy Mô hình và Xem Dashboard
Mở các file `.ipynb` (phần Model và Dashboard) bằng Jupyter Notebook hoặc Google Colab. Chạy toàn bộ các Cell để hệ thống tự động kéo dữ liệu mới nhất từ Cloud DB, thực hiện dự báo và render biểu đồ trực quan.

## 👥 Tác giả
* **Huỳnh Bá Duy** - AI/Data Engineer Intern | Trường Đại học Công nghệ Thông tin và Truyền thông Việt - Hàn (VKU).
