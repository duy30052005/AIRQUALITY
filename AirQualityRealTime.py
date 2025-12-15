import os
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import pytz
import sys

# ==============================================================================
# 1. CẤU HÌNH HỆ THỐNG
# ==============================================================================

API_KEY = os.getenv("OPENAQ_API_KEY", "42eedf3f60d586732ed805ef7cc217bdb2c01bdaa34556e28a68093db6f08113")
LOCATION_ID = 4946812

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
    print(f"❌ Lỗi cấu hình DB: {e}")

SENSOR_MAP = {
    13502163: "co", 13502162: "no2", 13502148: "o3",
    13502153: "pm10", 13502151: "pm25", 13502157: "so2"
}

# ==============================================================================
# 2. LOGIC ETL
# ==============================================================================
def run_realtime_job():
    # In ra ID của process để kiểm tra xem có bị chạy trùng lặp không
    print(f"\n🚀 [REAL-TIME] PID: {os.getpid()} - Bắt đầu quét...")
    
    hanoi_tz = pytz.timezone('Asia/Bangkok')
    
    # --- BƯỚC 1: EXTRACT ---
    url = f"https://api.openaq.org/v3/locations/{LOCATION_ID}/latest"
    headers = {"X-API-Key": API_KEY}
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code != 200:
            print(f"❌ API Lỗi: {response.status_code}")
            return
            
        data = response.json().get('results', [])
        if not data:
            print("⚠️ API trả về rỗng.")
            return

        # --- BƯỚC 2: SMART CHECK ---
        first_item_time = data[0].get('datetime', {}).get('local')
        if not first_item_time: return
        
        latest_api_dt = pd.to_datetime(first_item_time)
        api_date_key = int(latest_api_dt.strftime('%Y%m%d'))
        api_time_key = int(latest_api_dt.hour * 100 + latest_api_dt.minute)
        
        print(f"   🔎 Thời gian API: {latest_api_dt} (D:{api_date_key}, T:{api_time_key})")

        try:
            with engine.connect() as conn:
                loc_key_df = pd.read_sql(text(f"SELECT \"LocationKey\" FROM \"Dim_Location\" WHERE \"LocationID_Source\" = {LOCATION_ID}"), conn)
                if not loc_key_df.empty:
                    loc_key = loc_key_df.iloc[0]['LocationKey']
                    
                    sql_check = text("""
                        SELECT MAX("DateKey") as max_date, MAX("TimeKey") as max_time 
                        FROM "Fact_AirQuality"
                        WHERE "LocationKey" = :loc_key AND "DateKey" = :date_key
                    """)
                    result = conn.execute(sql_check, {"loc_key": int(loc_key), "date_key": api_date_key}).fetchone()
                    
                    if result and result[0] is not None:
                        db_max_time = int(result[1])
                        if api_time_key <= db_max_time:
                            print(f"   zzz Dữ liệu cũ (DB Time: {db_max_time}). BỎ QUA.")
                            return 
        except Exception as e:
            print(f"⚠️ Lỗi check DB: {e}")

        # --- BƯỚC 3: TRANSFORM ---
        print(f"   ✅ Dữ liệu MỚI! Xử lý {len(data)} chỉ số...")
        processed_rows = []
        
        with engine.connect() as conn:
            p_df = pd.read_sql(text('SELECT "ParameterName", "ParameterKey" FROM "Dim_Parameter"'), conn)
            l_df = pd.read_sql(text('SELECT "LocationID_Source", "LocationKey" FROM "Dim_Location"'), conn)
        
        param_db_map = dict(zip(p_df['ParameterName'], p_df['ParameterKey']))
        loc_db_map = dict(zip(l_df['LocationID_Source'], l_df['LocationKey']))

        for item in data:
            sensor_id = item.get('sensorsId')
            param_name = SENSOR_MAP.get(sensor_id)
            if not param_name: continue
                
            local_time_str = item.get('datetime', {}).get('local')
            dt_obj = pd.to_datetime(local_time_str)
            
            # Ép kiểu int ngay tại đây cho chắc chắn
            row = {
                'DateKey': int(dt_obj.strftime('%Y%m%d')),
                'TimeKey': int(dt_obj.hour * 100 + dt_obj.minute),
                'LocationKey': loc_db_map.get(LOCATION_ID),
                'ParameterKey': param_db_map.get(param_name),
                'SourceKey': 1,
                'Value': item.get('value')
            }
            if row['LocationKey'] and row['ParameterKey']:
                processed_rows.append(row)
        
        if not processed_rows: return
        df_fact = pd.DataFrame(processed_rows)
        
        # --- BƯỚC 4: LOAD (CỰC KỲ QUAN TRỌNG: XỬ LÝ SẠCH INT) ---
        print(f"   💾 Đang nạp {len(df_fact)} dòng...")
        
        # 1. Chuyển đổi Series sang List Python thuần túy
        # set() để loại bỏ trùng lặp
        # int(x) để ép kiểu python int
        unique_dates = sorted(list(set(int(x) for x in df_fact['DateKey'].unique())))
        unique_times = sorted(list(set(int(x) for x in df_fact['TimeKey'].unique())))
        
        if not unique_dates or not unique_times:
            return

        # 2. Tạo chuỗi String thủ công. 
        # Ví dụ: "20251215, 20251214"
        # Đảm bảo KHÔNG dùng numpy array ở đây
        date_str = ", ".join(str(x) for x in unique_dates)
        time_str = ", ".join(str(x) for x in unique_times)
        
        # DEBUG: In ra để kiểm tra xem còn chữ "np." không
        print(f"   🛠 DEBUG SQL IN: Dates=({date_str}) | Times=({time_str})")

        loc_key_val = int(loc_db_map.get(LOCATION_ID))
        
        # 3. Ráp vào câu SQL
        sql_clean = f"""
            DELETE FROM "Fact_AirQuality" 
            WHERE "LocationKey" = {loc_key_val}
            AND "DateKey" IN ({date_str})
            AND "TimeKey" IN ({time_str})
        """
        
        with engine.begin() as conn:
            conn.execute(text(sql_clean))
            df_fact.to_sql('Fact_AirQuality', conn, if_exists='append', index=False)
            
        print("   🎉 THÀNH CÔNG!")

    except Exception as e:
        print(f"❌ Lỗi hệ thống: {e}")
        import traceback
        traceback.print_exc()

# ==============================================================================
# 3. WEB SERVER
# ==============================================================================
app = Flask(__name__)

# Kiểm tra nếu scheduler chưa chạy thì mới start (tránh chạy 2 lần)
if not app.debug or os.environ.get('WERKZEUG_RUN_MAIN') == 'true':
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=run_realtime_job, trigger="interval", minutes=5)
    scheduler.start()

@app.route('/')
def index(): return "🌍 Service RUNNING."

@app.route('/update')
def manual():
    run_realtime_job()
    return "✅ Triggered update."

if __name__ == "__main__":
    # Chỉ chạy run_realtime_job ngay lập tức nếu không phải là bản reload của Flask
    run_realtime_job()
        
    port = int(os.environ.get("PORT", 5000))
    # use_reloader=False để tránh Flask chạy script 2 lần
    app.run(host='0.0.0.0', port=port, use_reloader=False)
