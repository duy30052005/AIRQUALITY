import os
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import sys
import time

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
    print(f"❌ [INIT] Lỗi cấu hình DB: {e}", flush=True)

SENSOR_MAP = {
    13502163: "co", 13502162: "no2", 13502148: "o3",
    13502153: "pm10", 13502151: "pm25", 13502157: "so2"
}

# ==============================================================================
# 2. LOGIC ETL (V3 - LOGGING CHI TIẾT)
# ==============================================================================
def run_realtime_job():
    start_time = time.time()
    current_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    pid = os.getpid()
    
    print(f"\n{'='*50}", flush=True)
    print(f"🚀 [START] Job PID: {pid} | Time: {current_time_str}", flush=True)

    # --- BƯỚC 1: EXTRACT ---
    url = f"https://api.openaq.org/v3/locations/{LOCATION_ID}/latest"
    headers = {"X-API-Key": API_KEY}

    try:
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code != 200:
            print(f"❌ [API] Lỗi Status Code: {response.status_code}", flush=True)
            return

        data = response.json().get('results', [])
        if not data:
            print("⚠️ [API] Trả về rỗng (0 records).", flush=True)
            return
            
        print(f"   📡 [API] Lấy thành công {len(data)} chỉ số.", flush=True)

        # --- BƯỚC 2: CHECK DỮ LIỆU ---
        first_item_time = data[0].get('datetime', {}).get('local')
        if not first_item_time: return

        latest_api_dt = pd.to_datetime(first_item_time)
        api_date_key = int(latest_api_dt.strftime('%Y%m%d'))
        api_time_key = int(latest_api_dt.hour * 100 + latest_api_dt.minute)

        print(f"   ⏰ [DATA TIME] {latest_api_dt} (DKey: {api_date_key}, TKey: {api_time_key})", flush=True)

        try:
            with engine.connect() as conn:
                loc_sql = text(f'SELECT "LocationKey" FROM "Dim_Location" WHERE "LocationID_Source" = {LOCATION_ID}')
                loc_key_df = pd.read_sql(loc_sql, conn)
                
                loc_key = None
                if not loc_key_df.empty:
                    loc_key = loc_key_df.iloc[0]['LocationKey']

                    sql_check = text("""
                        SELECT MAX("TimeKey") FROM "Fact_AirQuality" 
                        WHERE "LocationKey" = :loc_key AND "DateKey" = :date_key
                    """)
                    result = conn.execute(sql_check, {"loc_key": int(loc_key), "date_key": api_date_key}).fetchone()

                    if result and result[0] is not None:
                        db_max_time = int(result[0])
                        if api_time_key <= db_max_time:
                            print(f"   zzz [SKIP] Dữ liệu API ({api_time_key}) <= DB ({db_max_time}). Không cần cập nhật.", flush=True)
                            return
                        else:
                            print(f"   ⚡ [UPDATE] Có dữ liệu mới hơn DB ({api_time_key} > {db_max_time}).", flush=True)
        except Exception as e:
            print(f"⚠️ [CHECK FAIL] Lỗi kiểm tra trùng (Vẫn tiếp tục): {e}", flush=True)

        # --- BƯỚC 3: TRANSFORM ---
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

        # --- BƯỚC 4: LOAD (V3 - LOGGING) ---
        unique_dates = sorted(list(set(int(x) for x in df_fact['DateKey'].unique())))
        unique_times = sorted(list(set(int(x) for x in df_fact['TimeKey'].unique())))
        
        print(f"   💾 [DB LOAD] Chuẩn bị nạp {len(df_fact)} dòng. Dates: {unique_dates}", flush=True)

        # 4.1 XỬ LÝ DIM_DATE
        try:
            with engine.begin() as conn: 
                for d_key in unique_dates:
                    print(f"      📅 [DIM_DATE] Đang xử lý ngày: {d_key}...", flush=True)
                    d_str = str(d_key)
                    year = int(d_str[:4])
                    month = int(d_str[4:6])
                    day = int(d_str[6:])
                    
                    dt_temp = datetime(year, month, day)
                    date_val = dt_temp.strftime('%Y-%m-%d')
                    day_of_week = dt_temp.strftime('%A')

                    insert_dim_sql = text(f"""
                        INSERT INTO "Dim_Date" ("DateKey", "FullDate", "Day", "Month", "Year", "DayOfWeek")
                        VALUES ({d_key}, '{date_val}', {day}, {month}, {year}, '{day_of_week}')
                        ON CONFLICT ("DateKey") DO NOTHING
                    """)
                    conn.execute(insert_dim_sql)
            print(f"      ✅ [DIM_DATE] Hoàn tất đồng bộ ngày.", flush=True)

        except Exception as e_dim:
            print(f"      ❌ [DIM_DATE ERROR] {e_dim}", flush=True)

        # 4.2 XỬ LÝ FACT
        try:
            date_str = ", ".join(str(x) for x in unique_dates)
            time_str = ", ".join(str(x) for x in unique_times)
            loc_key_val = int(loc_db_map.get(LOCATION_ID))

            print(f"      🚜 [FACT] Đang xóa dữ liệu cũ (Clean up)...", flush=True)
            sql_clean = f"""
                DELETE FROM "Fact_AirQuality" 
                WHERE "LocationKey" = {loc_key_val} 
                AND "DateKey" IN ({date_str}) 
                AND "TimeKey" IN ({time_str})
            """

            with engine.begin() as conn:
                conn.execute(text(sql_clean))
                df_fact.to_sql('Fact_AirQuality', conn, if_exists='append', index=False)
            
            end_time = time.time()
            duration = round(end_time - start_time, 2)
            print(f"   🎉 [SUCCESS] Đã nạp xong Fact Table. Tổng thời gian: {duration}s", flush=True)
            
        except Exception as e_fact:
            print(f"   ❌ [FACT ERROR] Lỗi nạp Fact: {e_fact}", flush=True)

    except Exception as e:
        print(f"❌ [SYSTEM ERROR] {e}", flush=True)
        import traceback
        traceback.print_exc()
    
    print(f"{'='*50}\n", flush=True)

# ==============================================================================
# 3. WEB SERVER
# ==============================================================================
app = Flask(__name__)
scheduler = BackgroundScheduler()
scheduler.add_job(func=run_realtime_job, trigger="interval", minutes=10)
scheduler.start()

@app.route('/')
def index():
    return "🌍 Service RUNNING. Logs enabled."

@app.route('/update')
def manual():
    run_realtime_job()
    return "✅ Triggered manual update."

if __name__ == "__main__":
    print("⚡ [INIT] Server Starting...", flush=True)
    run_realtime_job()
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port, use_reloader=False)
