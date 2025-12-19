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
# 🔥 CẤU HÌNH LOGGING ĐỂ HIỆN THỊ NGAY LẬP TỨC
# ==============================================================================
# Ép Python không giữ log trong bộ đệm
try:
    sys.stdout.reconfigure(line_buffering=True)
except (AttributeError, Exception):
    pass
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger()

# ==============================================================================
# 1. CẤU HÌNH HỆ THỐNG
# ==============================================================================
DB_PASS = os.getenv("DB_PASSWORD", "Duy@12345")
DB_USER = "postgres.bkqhsxdynslfdtkcucij"
DB_HOST = "aws-1-ap-southeast-1.pooler.supabase.com"
DB_PORT = "6543"
DB_NAME = "postgres"

try:
    encoded_pass = quote_plus(DB_PASS)
    DB_URI = f"postgresql://{DB_USER}:{encoded_pass}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(DB_URI, pool_pre_ping=True)
    print("✅ Kết nối Database thành công!", flush=True)
except Exception as e:
    print(f"❌ Lỗi cấu hình DB: {e}", flush=True)

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
        except: continue

# ==============================================================================
# 3. JOB 1: AIR QUALITY ETL
# ==============================================================================
def run_air_quality_job():
    print(f"\n💨 [{datetime.now().strftime('%H:%M:%S')}] JOB 1: Bắt đầu lấy dữ liệu Air Quality...", flush=True)
    url = f"https://api.openaq.org/v3/locations/{LOCATION_ID_AQ}/latest"
    headers = {"X-API-Key": OPENAQ_API_KEY}
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        data = response.json().get('results', [])
        if not data:
            print("⚠️ Không có dữ liệu mới từ OpenAQ.", flush=True)
            return

        processed_rows = []
        with engine.connect() as conn:
            p_df = pd.read_sql(text('SELECT "ParameterName", "ParameterKey" FROM "Dim_Parameter"'), conn)
            l_df = pd.read_sql(text('SELECT "LocationID_Source", "LocationKey" FROM "Dim_Location"'), conn)
            
        param_db_map = dict(zip(p_df['ParameterName'], p_df['ParameterKey']))
        loc_db_map = dict(zip(l_df['LocationID_Source'], l_df['LocationKey']))
        loc_key = loc_db_map.get(LOCATION_ID_AQ)

        for item in data:
            sensor_id = item.get('sensorsId')
            param_name = SENSOR_MAP_AQ.get(sensor_id)
            if not param_name: continue
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
        df_fact = pd.DataFrame(processed_rows).drop_duplicates(subset=['DateKey', 'TimeKey', 'LocationKey', 'ParameterKey'])

        unique_dates = sorted(df_fact['DateKey'].unique())
        date_str = ", ".join(str(x) for x in unique_dates)

        with engine.begin() as conn:
            ensure_dim_date(conn, unique_dates)
            sql_clean = f'DELETE FROM "Fact_AirQuality" WHERE "LocationKey" = {loc_key} AND "DateKey" IN ({date_str})'
            conn.execute(text(sql_clean))
            df_fact.to_sql('Fact_AirQuality', conn, if_exists='append', index=False)
            print(f"✅ Đã lưu {len(df_fact)} dòng vào Fact_AirQuality.", flush=True)

    except Exception as e:
        print(f"❌ Lỗi Job 1: {e}", flush=True)
    gc.collect()

# ==============================================================================
# 4. JOB 2: WEATHER ETL
# ==============================================================================
def run_weather_job():
    print(f"\n☀️ [{datetime.now().strftime('%H:%M:%S')}] JOB 2: Bắt đầu lấy dữ liệu Weather...", flush=True)
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": LAT, "longitude": LON,
        "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m,rain,surface_pressure",
        "timezone": "Asia/Bangkok", "forecast_days": 3
    }

    try:
        response = requests.get(url, params=params, timeout=15)
        data = response.json()
        df = pd.DataFrame(data['hourly'])
        df['time'] = pd.to_datetime(df['time'])
        df['DateKey'] = df['time'].dt.strftime('%Y%m%d').astype(int)
        df['TimeKey'] = df['time'].dt.hour * 100
        df['LocationKey'] = LOCATION_KEY_WEATHER
        
        mapping = {'temperature_2m': 'Temperature', 'relative_humidity_2m': 'Humidity', 
                   'wind_speed_10m': 'WindSpeed', 'rain': 'Rain', 'surface_pressure': 'Pressure'}
        df.rename(columns=mapping, inplace=True)
        fact_df = df[['DateKey', 'TimeKey', 'LocationKey', 'Temperature', 'Humidity', 'WindSpeed', 'Rain', 'Pressure']]

        unique_dates = sorted(fact_df['DateKey'].unique())
        date_str = ", ".join(str(x) for x in unique_dates)

        with engine.begin() as conn:
            ensure_dim_date(conn, unique_dates)
            conn.execute(text(f'DELETE FROM "Fact_Weather" WHERE "LocationKey"={LOCATION_KEY_WEATHER} AND "DateKey" IN ({date_str})'))
            fact_df.to_sql('Fact_Weather', conn, if_exists='append', index=False)
            print(f"✅ Đã lưu {len(fact_df)} dòng vào Fact_Weather.", flush=True)
    except Exception as e:
        print(f"❌ Lỗi Job 2: {e}", flush=True)
    gc.collect()

# ==============================================================================
# 5. SERVER & SCHEDULER
# ==============================================================================
app = Flask(__name__)

@app.route('/')
def index():
    return f"🌍 AI Service Active. Time: {datetime.now()}"

scheduler = BackgroundScheduler()
# Chạy Job 1 mỗi 10 phút, Job 2 mỗi 60 phút
scheduler.add_job(func=run_air_quality_job, trigger="interval", minutes=10)
scheduler.add_job(func=run_weather_job, trigger="interval", minutes=60)
scheduler.start()

if __name__ == "__main__":
    print("⚡ Khởi động hệ thống và quét dữ liệu lần đầu...", flush=True)
    run_air_quality_job()
    run_weather_job()
    
    port = int(os.environ.get("PORT", 5000))
    # use_reloader=False là bắt buộc khi dùng APScheduler để tránh chạy trùng job
    app.run(host='0.0.0.0', port=port, use_reloader=False)
