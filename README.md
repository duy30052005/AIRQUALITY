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

| Biểu đồ Dự báo (Model Forecast) |
|:---:|
| ![Model](<img width="804" height="600" alt="image" src="https://github.com/user-attachments/assets/b2bba782-ee75-4681-a3a7-b9932339ee7c" />
) 

## 🛠 Cách triển khai (How to run)
1. Cài đặt các thư viện cần thiết:
   ```bash
git clone [https://github.com/duy30052005/AIRQUALITY.git](https://github.com/duy30052005/AIRQUALITY.git)
cd AIRQUALITY
pip install -r requirements.txt
