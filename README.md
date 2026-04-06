# 🌍 Hanoi Air Quality: Real-time Data Warehouse & AI Forecasting

## 📝 Giới thiệu
Dự án là một hệ thống AI End-to-End thực hiện việc giám sát, phân tích và dự báo chất lượng không khí tại Hà Nội. Hệ thống tự động thu thập dữ liệu thời gian thực (Real-time ETL), lưu trữ tối ưu trên Cloud Data Warehouse (Supabase) và áp dụng các kỹ thuật Học máy (Machine Learning) tiên tiến để dự báo mức độ ô nhiễm trong tương lai, hỗ trợ đưa ra cảnh báo sớm.

## 🚀 Công nghệ và Kỹ thuật cốt lõi
* **Data Engineering (ETL Pipeline):** Python (Pandas, SQLAlchemy), Flask, APScheduler.
* **Database:** PostgreSQL (Supabase) tối ưu hóa Connection Pool cho môi trường Cloud.
* **Machine Learning / AI:** * *Gradient Boosting Regressor (GBR):* Dự báo nồng độ khí dựa trên sự tương tác phức tạp của thời tiết.
  * *Facebook Prophet:* Phân tích chuỗi thời gian (Time-series) và dự báo xu hướng với Logistic Growth.
* **Feature Engineering:** Kỹ thuật trễ (Lags), Trung bình trượt (Rolling Mean), và Mã hóa chu kỳ thời gian (Cyclical Encoding với Sin/Cos).
* **Visualization:** Matplotlib nâng cao (hiển thị dữ liệu thực tế, dữ liệu nội suy và dự báo tương lai).

## 💡 Các tính năng nổi bật

### 1. Luồng dữ liệu tự động (Real-Time ETL Pipeline)
* **Trích xuất (Extract):** Tự động gọi API từ **OpenAQ** (Dữ liệu không khí) và **Open-Meteo** (Dữ liệu thời tiết tương lai) mỗi 15 - 60 phút.
* **Biến đổi (Transform):** Xử lý múi giờ (Asia/Bangkok), chuẩn hóa kiểu dữ liệu và ánh xạ thông minh các Dimension keys.
* **Nạp (Load):** Cơ chế "Smart Check" và `DELETE -> INSERT` giúp ngăn chặn tuyệt đối tình trạng lặp dữ liệu (Duplication) khi chạy trên server liên tục.

### 2. Kỹ thuật Feature Engineering chuyên sâu
Mô hình Học máy không chỉ sử dụng dữ liệu thô mà được huấn luyện trên tập đặc trưng (Features) đã qua xử lý phức tạp:
* Tính toán chỉ số đọng khí (Stagnation Index) và điểm sương (Dew Point).
* Áp dụng mã hóa lượng giác (Sine/Cosine transformation) cho Giờ và Ngày trong tuần để mô hình hiểu được tính chu kỳ tuần hoàn của thời gian.

### 3. Hệ thống Dự báo Kép (Dual Forecasting System)
* **Backend Forecast (GBR):** Huấn luyện mô hình Gradient Boosting với hàm mất mát Huber (chống nhiễu), kết hợp dữ liệu thời tiết tương lai để dự báo nồng độ PM2.5, PM10, CO, NO2, SO2, O3 và lưu trực tiếp vào Database.
* **Frontend Forecast (Prophet):** Xử lý mượt mà các khoảng trống dữ liệu bằng nội suy, áp dụng hàm Log Transform và giới hạn trần/sàn (Cap/Floor) để dự báo xu hướng biến động dài hạn trên Dashboard.

## 📂 Cấu trúc Repository
* `AirQualityRealTime.py`: Khối Web Server (Flask) & Scheduler quản lý toàn bộ luồng ETL tự động trên Production (Render).
* Các file `.ipynb`: Chứa mã nguồn huấn luyện mô hình Gradient Boosting Regressor, Facebook Prophet và giao diện Dashboard cảnh báo.
* `requirements.txt`: Danh sách các thư viện môi trường cần thiết.

## 📊 Giao diện & Kết quả

*(Kéo thả hình ảnh Dashboard trực quan và Biểu đồ dự báo thực tế của bạn vào đây)*

| Dashboard Dự báo (Prophet & Matplotlib) | Cấu trúc ETL Logic |
|:---:|:---:|
| ![Dashboard](link_anh_dashboard) | ![ETL](link_anh_minh_hoa_etl) |

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
Mở các file `.ipynb` (phần Model và Dashboard) bằng Jupyter Notebook. Chạy toàn bộ các Cell để hệ thống tự động kéo dữ liệu mới nhất từ Cloud DB, thực hiện dự báo và render biểu đồ trực quan.

## 👥 Tác giả
* **Huỳnh Bá Duy** - AI Engineer Intern | Trường Đại học Công nghệ Thông tin và Truyền thông Việt - Hàn (VKU).
