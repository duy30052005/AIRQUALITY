import os
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import sys
import psutil 
import gc     

# ==============================================================================
# 🔥 CẤU HÌNH QUAN TRỌNG CHO RENDER LOGS
# ==============================================================================
# Ép Python in log ra ngay lập tức (Unbuffered), không chờ đầy bộ nhớ mới in
# Giúp bạn thấy log realtime trên Render dashboard
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

# ==============================================================================
# 1. CẤU HÌNH HỆ THỐNG
# ==============================================================================

# --- DATABASE ---
DB_PASS = os.getenv("DB_PASSWORD", "Duy@12345")
DB_USER = "postgres.bkqhsxdynslfdtkcucij"
DB_HOST = "aws-1-ap-southeast-1.pooler.supabase.com"
DB_PORT = "6543"
DB_NAME = "postgres"

try:
    encoded_pass = quote_plus(DB_PASS)
    DB_URI = f"postgresql://{DB_USER}:{encoded_pass}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(DB_URI, pool_pre_ping=True)
except Exception as e:
    # Không cần flush=True nữa vì đã cấu hình ở trên
    print(f"❌ Lỗi cấu hình DB: {e}") 

# --- CONFIG API ---
OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY", "42eedf3f60d586732ed805ef7cc217bdb2c01bdaa34556e28a68093db6f08113")
LOCATION_ID_AQ = 4946812
SENSOR_MAP_AQ = {
    13502163: "co", 13502162: "no2", 13502148: "o3",
    13502153: "pm10", 13502151: "pm25", 13502157: "so2"
}

LAT = 21.02
LON = 105.85
LOCATION_KEY_WEATHER = 1 
FORECAST_DAYS = 3 

# ==============================================================================
# 2. HÀM MONITORING & HELPER
# ==============================================================================
def log_resources(tag=""):
    """In ra lượng RAM và CPU đang sử dụng"""
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info().rss / 1024 / 1024 
    cpu_usage = process.cpu_percent(interval=None) 
    print(f"📊 [{tag}] RAM: {mem_info:.2f} MB | CPU: {cpu_usage}%")

def ensure_dim_date(conn, unique_dates):
    """Kiểm tra và tự động tạo ngày mới trong Dim_Date"""
    for d_key in unique_dates:
        exists = conn.execute(text(f'SELECT 1 FROM "Dim_Date" WHERE "DateKey" = {d_key}')).fetchone()
        if not exists:
            d_str = str(d_key)
            year, month, day = int(d_str[:4]), int(d_str[4:6]), int(d_str[6:])
            date_val = f"{year}-{month:02d}-{day:02d}"
            dt_temp = datetime(year, month, day)
            day_of_week = dt_temp.strftime('%A')
            
            sql = text(f"""
                INSERT INTO "Dim_Date" ("DateKey", "FullDate", "Day", "Month", "Year", "DayOfWeek")
                VALUES ({d_key}, '{date_val}', {day}, {month}, {year}, '{day_of_week}')
                ON CONFLICT ("DateKey") DO NOTHING
            """)
            conn.execute(sql)

# ==============================================================================
# 3. JOB 1: AIR QUALITY
# ==============================================================================
def run_air_quality_job():
    log_resources("AQ-Start") 
    print(f"\n💨 [AIR QUALITY] Start: {datetime.now()}")
    
    url = f"https://api.openaq.org/v3/locations/{LOCATION_ID_AQ}/latest"
    headers = {"X-API-Key": OPENAQ_API_KEY}
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code != 200: return
        data = response.json().get('results', [])
        if not data: return
        
        # --- Transform ---
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

            local_time_str = item.get('datetime', {}).get('local')
            dt_obj = pd.to_datetime(local_time_str)
            
            row = {
                'DateKey': int(dt_obj.strftime('%Y%m%d')),
                'TimeKey': int(dt_obj.hour * 100 + dt_obj.minute),
                'LocationKey': loc_key,
                'ParameterKey': param_db_map.get(param_name),
                'SourceKey': 1,
                'Value': item.get('value')
            }
            if row['LocationKey'] and row['ParameterKey']:
                processed_rows.append(row)
        
        if not processed_rows: return
        df_fact = pd.DataFrame(processed_rows)
        
        log_resources("AQ-Pandas-Loaded") 

        # --- Load ---
        unique_dates = sorted(df_fact['DateKey'].unique())
        unique_times = sorted(df_fact['TimeKey'].unique())
        date_str = ", ".join(str(x) for x in unique_dates)
        time_str = ", ".join(str(x) for x in unique_times)

        with engine.begin() as conn:
            ensure_dim_date(conn, unique_dates)
            sql_clean = f"""
                DELETE FROM "Fact_AirQuality"
                WHERE "LocationKey" = {loc_key}
                AND "DateKey" IN ({date_str}) AND "TimeKey" IN ({time_str})
            """
            conn.execute(text(sql_clean))
            df_fact.to_sql('Fact_AirQuality', conn, if_exists='append', index=False)
            print(f"   🎉 AirQuality: Done {len(df_fact)} rows.")

        # --- Dọn dẹp RAM ---
        del df_fact          
        del processed_rows   
        gc.collect()         
        log_resources("AQ-End-Cleaned")

    except Exception as e:
        print(f"❌ AirQuality Error: {e}")

# ==============================================================================
# 4. JOB 2: WEATHER FORECAST
# ==============================================================================
def run_weather_job():
    log_resources("Weather-Start")
    print(f"\n☀️ [WEATHER] Start: {datetime.now()}")

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": LAT, "longitude": LON,
        "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m,rain,surface_pressure,cloud_cover",
        "timezone": "Asia/Bangkok",
        "forecast_days": FORECAST_DAYS
    }

    try:
        response = requests.get(url, params=params, timeout=15)
        if response.status_code != 200: return
        data = response.json()
        
        # --- Transform ---
        df = pd.DataFrame(data['hourly'])
        df['time'] = pd.to_datetime(df['time'])
        df['DateKey'] = df['time'].dt.strftime('%Y%m%d').astype(int)
        df['TimeKey'] = df['time'].dt.hour * 100 + df['time'].dt.minute
        df['LocationKey'] = LOCATION_KEY_WEATHER
        
        mapping = {
            'temperature_2m': 'Temperature', 'relative_humidity_2m': 'Humidity',
            'wind_speed_10m': 'WindSpeed', 'rain': 'Rain',
            'surface_pressure': 'Pressure', 'cloud_cover': 'CloudCover'
        }
        df.rename(columns=mapping, inplace=True)
        
        fact_df = df[['DateKey', 'TimeKey', 'LocationKey', 'Temperature', 
                      'Humidity', 'WindSpeed', 'Rain', 'Pressure', 'CloudCover']]
        
        log_resources("Weather-Pandas-Loaded")

        # --- Load ---
        unique_dates = sorted(fact_df['DateKey'].unique())
        date_str = ", ".join(str(x) for x in unique_dates)

        with engine.begin() as conn:
            ensure_dim_date(conn, unique_dates)
            if date_str:
                sql_clean = f"""
                    DELETE FROM "Fact_Weather"
                    WHERE "LocationKey" = {LOCATION_KEY_WEATHER}
                    AND "DateKey" IN ({date_str})
                """
                conn.execute(text(sql_clean))
            fact_df.to_sql('Fact_Weather', conn, if_exists='append', index=False)
            print(f"   🎉 Weather: Done {len(fact_df)} rows.")

        # --- Dọn dẹp RAM ---
        del df, fact_df, data 
        gc.collect()          
        log_resources("Weather-End-Cleaned")

    except Exception as e:
        print(f"❌ Weather Error: {e}")

# ==============================================================================
# 5. MAIN
# ==============================================================================
app = Flask(__name__)
scheduler = BackgroundScheduler()
scheduler.add_job(func=run_air_quality_job, trigger="interval", minutes=10)
scheduler.add_job(func=run_weather_job, trigger="interval", minutes=60)
scheduler.start()

@app.route('/')
def index():
    log_resources("Web-Check")
    return "🌍 Service RUNNING. Logs are unbuffered."

if __name__ == "__main__":
    print("⚡ Kích hoạt lần quét đầu tiên...")
    run_air_quality_job()
    run_weather_job()
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port, use_reloader=False)
