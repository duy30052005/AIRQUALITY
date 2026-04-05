# 🌍 Hanoi Air Quality Data Warehouse & Forecasting

## 📝 Giới thiệu
Dự án xây dựng một hệ thống hoàn chỉnh từ khâu thu thập dữ liệu thời gian thực, xử lý ETL đến ứng dụng phân tích và dự đoán chất lượng không khí tại thành phố Hà Nội, Việt Nam. Hệ thống hỗ trợ giám sát tức thời và dự báo xu hướng ô nhiễm để cảnh báo sớm.

## 🚀 Công nghệ và Công cụ
* **Data Engineering (ETL):** Python (Pandas), tự động hóa thu thập dữ liệu.
* **Machine Learning:** Mô hình học máy dự báo chuỗi thời gian (Gradient Boosting Regressor / Prophet).
* **Visualization:** Giao diện Dashboard trực quan hóa dữ liệu thời gian thực.

## 📂 Cấu trúc Repository hiện tại
* `AirQualityRealTime.py`: Script triển khai trên server để tự động kết nối, thu thập và cập nhật dữ liệu chất lượng không khí theo thời gian thực (Real-time Data Ingestion).
* `ETL_real_time.ipynb`: Notebook thực hiện quy trình ETL (Trích xuất - Biến đổi - Nạp) và tích hợp giao diện Dashboard giám sát.
* `GBRmodel.ipynb`: Notebook chứa mã nguồn huấn luyện mô hình Học máy để dự báo xu hướng nồng độ các chất ô nhiễm.
* `requirements.txt`: Danh sách các thư viện Python cần thiết (dependencies) để triển khai dự án.

## 📊 Hình ảnh hệ thống và Kết quả

*(Duy nhớ Upload 1-2 tấm ảnh giao diện Dashboard hoặc biểu đồ dự báo của mô hình lên GitHub, sau đó chèn link ảnh vào đây để nhà tuyển dụng xem nhé)*

| Biểu đồ Dự báo (Model Forecast) | Giao diện Dashboard (Real-time) |
|:---:|:---:|
| ![Model](link_anh_model_cua_ban) | ![Dashboard](link_anh_dashboard_cua_ban) |

## 🛠 Cách triển khai (How to run)
1. Cài đặt các thư viện cần thiết:
   ```bash
   pip install -r requirements.txt
