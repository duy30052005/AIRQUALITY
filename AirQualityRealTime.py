import os
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import sys
import gc
import logging

# ==============================================================================
# 🔥 CẤU HÌNH LOGGING (QUAN TRỌNG CHO RENDER)
# ==============================================================================
# Ép Python đẩy log ra ngay lập tức (không buffer)
try:
    sys.stdout.reconfigure(line_buffering=True)
except (AttributeError, Exception):
    pass

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger()

# ==============================================================================
# 1. CẤU HÌNH KẾT NỐI DATABASE
# ==============================================================================
# Khuyến nghị: Nên đặt DB_PASSWORD trong Environment Variables của Render
DB_PASS = os.getenv("DB_PASSWORD", "Duy@12345") 
DB_USER = "postgres.bkqhsxdynslfdtkcucij"
DB_HOST = "aws-1-ap-southeast-1.pooler.supabase.com"
DB_PORT = "6543"
DB_NAME = "postgres"

try:
    encoded_pass = quote_plus(DB_PASS)
    # pool_pre_ping=True giúp tự động kết nối lại nếu DB ngắt kết nối
    DB_URI = f"postgresql://{DB_USER}:{encoded_pass}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(DB_URI, pool_pre_ping=True)
    logger.info("✅ Kết nối Database thành công!")
except Exception as e:
    logger.error(f"❌ Lỗi cấu hình DB: {e}")

# Cấu hình API & Vị trí
OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY", "42eedf3f60d586732ed805ef7cc217bdb2c01bdaa34556e28a68093db6f08113")
LOCATION_ID_AQ = 4946812
SENSOR_MAP_AQ = {
    13502163: "co", 13502162: "no2", 13502148: "o3",
    13502153: "pm10", 13502151: "pm25", 13502157: "so2"
}
LAT, LON = 21.02, 105.85
LOCATION_KEY_WEATHER = 1 

# ==============================================================================
# 2. HELPER FUNCTIONS
# ==============================================================================
def ensure_dim_date(conn, unique_dates):
    """Đảm bảo các ngày có trong Dim_Date để tránh lỗi khóa ngoại"""
    for d_key in unique_dates:
        try:
            d_key_int = int(d_key)
            exists = conn.execute(text(f'SELECT 1 FROM "Dim_Date" WHERE "DateKey" = {d_key_int}')).fetchone()
            if not exists:
                dt = datetime.strptime(str(d_key_int), '%Y%m%d')
                conn.execute(text(f"""
                    INSERT INTO "Dim_Date" ("DateKey", "FullDate", "Day", "Month", "Year", "DayOfWeek")
                    VALUES (:k, :fd, :d, :m, :y, :dow)
                """), {"k": d_key_int, "fd": dt.date(), "d": dt.day, "m": dt.month, "y": dt.year, "dow": dt.strftime('%A')})
                logger.info(f"📅 Đã tạo ngày mới trong Dim_Date: {d_key_int}")
        except Exception as e:
            logger.warning(f"⚠️ Lỗi tạo Dim_Date {d_key}: {e}")
            continue

# ==============================================================================
# 3. JOB 1: AIR QUALITY ETL (ĐÃ FIX LỖI XÓA DỮ LIỆU)
# ==============================================================================
def run_air_quality_job():
    logger.info("💨 JOB 1: Bắt đầu lấy dữ liệu Air Quality...")
    url = f"https://api.openaq.org/v3/locations/{LOCATION_ID_AQ}/latest"
    headers = {"X-API-Key": OPENAQ_API_KEY}
    
    try:
        response = requests.get(url, headers=headers, timeout=20)
        if response.status_code != 200:
            logger.error(f"❌ Lỗi API OpenAQ: {response.status_code} - {response.text}")
            return

        data = response.json().get('results', [])
        if not data:
            logger.warning("⚠️ Không có dữ liệu mới từ OpenAQ.")
            return

        processed_rows = []
        
        # Lấy Mapping từ DB
        with engine.connect() as conn:
            p_df = pd.read_sql(text('SELECT "ParameterName", "ParameterKey" FROM "Dim_Parameter"'), conn)
            l_df = pd.read_sql(text('SELECT "LocationID_Source", "LocationKey" FROM "Dim_Location"'), conn)
            
        param_db_map = dict(zip(p_df['ParameterName'], p_df['ParameterKey']))
        loc_db_map = dict(zip(l_df['LocationID_Source'], l_df['LocationKey']))
        loc_key = loc_db_map.get(LOCATION_ID_AQ)

        if not loc_key:
            logger.error(f"❌ Không tìm thấy LocationKey cho ID {LOCATION_ID_AQ}")
            return

        # Xử lý dữ liệu
        for item in data:
            sensor_id = item.get('sensorsId')
            param_name = SENSOR_MAP_AQ.get(sensor_id)
            if not param_name: continue
            
            # Lấy giờ Local (quan trọng để đúng múi giờ VN)
            dt_obj = pd.to_datetime(item.get('datetime', {}).get('local'))
            
            processed_rows.append({
                'DateKey': int(dt_obj.strftime('%Y%m%d')),
                'TimeKey': int(dt_obj.hour * 100 + dt_obj.minute),
                'LocationKey': loc_key,
                'ParameterKey': param_db_map.get(param_name),
                'SourceKey': 1,
                'Value': item.get('value')
            })
        
        if not processed_rows: return
        
        # DataFrame dữ liệu mới
        df_fact = pd.DataFrame(processed_rows).drop_duplicates(subset=['DateKey', 'TimeKey', 'LocationKey', 'ParameterKey'])
        unique_dates = df_fact['DateKey'].unique().tolist()
        date_str = ", ".join(str(x) for x in unique_dates)

        # --- PHẦN SỬA LỖI QUAN TRỌNG: CHECK TRÙNG TRƯỚC KHI INSERT ---
        with engine.connect() as conn:
            ensure_dim_date(conn, unique_dates)
            
            # 1. Tìm dữ liệu đã có trong DB
            query_check = text(f"""
                SELECT "DateKey", "TimeKey", "ParameterKey" 
                FROM "Fact_AirQuality" 
                WHERE "LocationKey" = {loc_key} AND "DateKey" IN ({date_str})
            """)
            existing_df = pd.read_sql(query_check, conn)
        
        # 2. Lọc bỏ dòng đã tồn tại
        if not existing_df.empty:
            existing_df['exists'] = True
            keys = ['DateKey', 'TimeKey', 'ParameterKey']
            # Left Join: Giữ lại bên trái (Mới), ghép với bên phải (Cũ)
            df_merged = pd.merge(df_fact, existing_df, on=keys, how='left')
            # Chỉ lấy dòng mà bên phải không có dữ liệu (exists is NaN)
            df_final = df_merged[df_merged['exists'].isna()].drop(columns=['exists'])
        else:
            df_final = df_fact

        # 3. Insert dòng mới
        if not df_final.empty:
            with engine.begin() as conn:
                df_final.to_sql('Fact_AirQuality', conn, if_exists='append', index=False)
                logger.info(f"✅ Đã thêm {len(df_final)} dòng mới vào Fact_AirQuality.")
        else:
            logger.info("⚡ Dữ liệu đã tồn tại, không cần cập nhật.")

    except Exception as e:
        logger.error(f"❌ Lỗi Job 1 (AQI): {e}")
    
    gc.collect()

# ==============================================================================
# 4. JOB 2: WEATHER ETL
# ==============================================================================
def run_weather_job():
    logger.info("☀️ JOB 2: Bắt đầu lấy dữ liệu Weather...")
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": LAT, "longitude": LON,
        "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m,rain,surface_pressure",
        "timezone": "Asia/Bangkok", 
        "forecast_days": 3
    }

    try:
        response = requests.get(url, params=params, timeout=20)
        if response.status_code != 200:
             logger.error(f"❌ Lỗi API Weather: {response.status_code}")
             return

        data = response.json()
        df = pd.DataFrame(data['hourly'])
        df['time'] = pd.to_datetime(df['time'])
        
        # Tạo key
        df['DateKey'] = df['time'].dt.strftime('%Y%m%d').astype(int)
        df['TimeKey'] = df['time'].dt.hour * 100
        df['LocationKey'] = LOCATION_KEY_WEATHER
        
        mapping = {
            'temperature_2m': 'Temperature', 'relative_humidity_2m': 'Humidity', 
            'wind_speed_10m': 'WindSpeed', 'rain': 'Rain', 'surface_pressure': 'Pressure'
        }
        df.rename(columns=mapping, inplace=True)
        fact_df = df[['DateKey', 'TimeKey', 'LocationKey', 'Temperature', 'Humidity', 'WindSpeed', 'Rain', 'Pressure']]

        unique_dates = sorted(fact_df['DateKey'].unique())
        date_str = ", ".join(str(x) for x in unique_dates)

        with engine.begin() as conn:
            ensure_dim_date(conn, unique_dates)
            # Với dữ liệu dự báo, việc ghi đè (Delete -> Insert) là chấp nhận được để cập nhật dự báo mới nhất
            conn.execute(text(f'DELETE FROM "Fact_Weather" WHERE "LocationKey"={LOCATION_KEY_WEATHER} AND "DateKey" IN ({date_str})'))
            fact_df.to_sql('Fact_Weather', conn, if_exists='append', index=False)
            logger.info(f"✅ Đã cập nhật {len(fact_df)} dòng dự báo thời tiết.")
            
    except Exception as e:
        logger.error(f"❌ Lỗi Job 2 (Weather): {e}")
    gc.collect()

# ==============================================================================
# 5. SERVER & SCHEDULER
# ==============================================================================
app = Flask(__name__)

@app.route('/')
def index():
    return f"🌍 AI Data Service Active. Time: {datetime.now()}"

# Cấu hình Scheduler
scheduler = BackgroundScheduler()
# Chạy Job 1 mỗi 15 phút (để tránh spam API và phù hợp với tần suất sensor)
scheduler.add_job(func=run_air_quality_job, trigger="interval", minutes=15)
# Chạy Job 2 mỗi 60 phút
scheduler.add_job(func=run_weather_job, trigger="interval", minutes=60)
scheduler.start()

if __name__ == "__main__":
    logger.info("⚡ Khởi động hệ thống...")
    
    # Chạy ngay 1 lần khi khởi động để có dữ liệu
    run_air_quality_job()
    run_weather_job()
    
    port = int(os.environ.get("PORT", 5000))
    # use_reloader=False là bắt buộc với APScheduler
    app.run(host='0.0.0.0', port=port, use_reloader=False)
