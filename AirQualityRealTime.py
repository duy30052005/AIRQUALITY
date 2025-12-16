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

# Ưu tiên lấy từ biến môi trường (cho Render), nếu không có thì dùng mặc định (cho Local/Colab)
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
# 2. LOGIC ETL (AUTO-FIX DIM_DATE & FACT)
# ==============================================================================
def run_realtime_job():
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
            print(f"⚠️ Lỗi check DB (vẫn tiếp tục): {e}")

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
        
        # --- BƯỚC 4: LOAD (AUTO-FIX DIM_DATE & CLEAN INT) ---
        print(f"   💾 Đang nạp {len(df_fact)} dòng...")
        
        unique_dates = sorted(list(set(int(x) for x in df_fact['DateKey'].unique())))
        unique_times = sorted(list(set(int(x) for x in df_fact['TimeKey'].unique())))
        
        if not unique_dates or not unique_times: return

        # ---------------------------------------------------------
        # 🔥 QUAN TRỌNG: TỰ ĐỘNG TẠO NGÀY MỚI TRONG DIM_DATE
        # ---------------------------------------------------------
        try:
            with engine.begin() as conn:
                for d_key in unique_dates:
                    # Kiểm tra xem ngày này đã có trong Dim_Date chưa
                    exists = conn.execute(text(f'SELECT 1 FROM "Dim_Date" WHERE "DateKey" = {d_key}')).fetchone()
                    
                    if not exists:
                        print(f"   ⚠️ Phát hiện ngày mới {d_key}. Đang tạo trong Dim_Date...")
                        
                        # Logic tạo thông tin ngày
                        d_str = str(d_key) # Ví dụ "20251216"
                        year = int(d_str[:4])
                        month = int(d_str[4:6])
                        day = int(d_str[6:])
                        date_val = f"{year}-{month:02d}-{day:02d}"
                        quarter = (month - 1) // 3 + 1
                        
                        # Câu lệnh INSERT (Lưu ý: Đảm bảo tên cột khớp với DB của bạn)
                        insert_dim_sql = text(f"""
                            INSERT INTO "Dim_Date" ("DateKey", "FullDate", "Day", "Month", "Year", "Quarter") 
                            VALUES ({d_key}, '{date_val}', {day}, {month}, {year}, {quarter})
                        """)
                        conn.execute(insert_dim_sql)
                        print(f"   ✅ Đã thêm ngày {d_key} vào Dim_Date.")
        except Exception as e_dim:
            print(f"❌ Lỗi cập nhật Dim_Date (Kiểm tra lại tên cột): {e_dim}")
            # Nếu lỗi tạo ngày, có thể sẽ lỗi Fact sau đó, nhưng cứ để chạy tiếp
        
        # ---------------------------------------------------------
        # NẠP FACT TABLE
        # ---------------------------------------------------------
        date_str = ", ".join(str(x) for x in unique_dates)
        time_str = ", ".join(str(x) for x in unique_times)
        
        loc_key_val = int(loc_db_map.get(LOCATION_ID))
        
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
# 3. WEB SERVER & SCHEDULER
# ==============================================================================
app = Flask(__name__)

# Khởi tạo Scheduler (chạy ngầm mỗi 30p)
scheduler = BackgroundScheduler()
scheduler.add_job(func=run_realtime_job, trigger="interval", minutes=30)
scheduler.start()

@app.route('/')
def index(): return "🌍 Service RUNNING."

@app.route('/update')
def manual():
    run_realtime_job()
    return "✅ Triggered update."

if __name__ == "__main__":
    # Chạy 1 lần ngay lập tức khi khởi động
    print("⚡ Kích hoạt lần quét đầu tiên...")
    run_realtime_job()
    
    port = int(os.environ.get("PORT", 5000))
    # use_reloader=False để tránh chạy 2 lần trên Local/Colab
    app.run(host='0.0.0.0', port=port, use_reloader=False)
