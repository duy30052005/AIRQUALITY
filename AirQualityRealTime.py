import os
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import sys

# ==============================================================================
# 1. CẤU HÌNH HỆ THỐNG
# ==============================================================================

# Ưu tiên lấy từ biến môi trường (cho Render), nếu không có thì dùng mặc định
API_KEY = os.getenv("OPENAQ_API_KEY", "42eedf3f60d586732ed805ef7cc217bdb2c01bdaa34556e28a68093db6f08113")
LOCATION_ID = 4946812

# Cấu hình Database Supabase
DB_PASS = os.getenv("DB_PASSWORD", "Duy@12345")
DB_USER = "postgres.bkqhsxdynslfdtkcucij"
DB_HOST = "aws-1-ap-southeast-1.pooler.supabase.com"
DB_PORT = "6543"
DB_NAME = "postgres"

try:
    encoded_pass = quote_plus(DB_PASS)
    DB_URI = f"postgresql://{DB_USER}:{encoded_pass}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    # pool_pre_ping=True giúp tự động kết nối lại nếu DB ngắt kết nối
    engine = create_engine(DB_URI, pool_pre_ping=True)
except Exception as e:
    print(f"❌ Lỗi cấu hình DB: {e}", flush=True)

# Mapping ID từ OpenAQ sang tên Parameter trong DB
SENSOR_MAP = {
    13502163: "co", 13502162: "no2", 13502148: "o3",
    13502153: "pm10", 13502151: "pm25", 13502157: "so2"
}

# ==============================================================================
# 2. LOGIC ETL (AUTO-FIX DIM_DATE & FACT - FINAL VERSION)
# ==============================================================================
def run_realtime_job():
    pid = os.getpid()
    print(f"\n🚀 [REAL-TIME] PID: {pid} - Bắt đầu quét dữ liệu...", flush=True)

    # --- BƯỚC 1: EXTRACT (Lấy dữ liệu từ API) ---
    url = f"https://api.openaq.org/v3/locations/{LOCATION_ID}/latest"
    headers = {"X-API-Key": API_KEY}

    try:
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code != 200:
            print(f"❌ API Lỗi: {response.status_code}", flush=True)
            return

        data = response.json().get('results', [])
        if not data:
            print("⚠️ API trả về rỗng.", flush=True)
            return

        # --- BƯỚC 2: SMART CHECK (Kiểm tra trùng lặp) ---
        first_item_time = data[0].get('datetime', {}).get('local')
        if not first_item_time: return

        latest_api_dt = pd.to_datetime(first_item_time)
        api_date_key = int(latest_api_dt.strftime('%Y%m%d')) # VD: 20251217
        api_time_key = int(latest_api_dt.hour * 100 + latest_api_dt.minute) # VD: 1305

        print(f"   🔎 Thời gian API: {latest_api_dt} (DateKey:{api_date_key}, TimeKey:{api_time_key})", flush=True)

        try:
            with engine.connect() as conn:
                # Lấy LocationKey từ DB
                loc_sql = text(f'SELECT "LocationKey" FROM "Dim_Location" WHERE "LocationID_Source" = {LOCATION_ID}')
                loc_key_df = pd.read_sql(loc_sql, conn)
                
                loc_key = None
                if not loc_key_df.empty:
                    loc_key = loc_key_df.iloc[0]['LocationKey']

                    # Kiểm tra xem dữ liệu giờ này đã có chưa
                    sql_check = text("""
                        SELECT MAX("TimeKey")
                        FROM "Fact_AirQuality" 
                        WHERE "LocationKey" = :loc_key AND "DateKey" = :date_key
                    """)
                    result = conn.execute(sql_check, {"loc_key": int(loc_key), "date_key": api_date_key}).fetchone()

                    if result and result[0] is not None:
                        db_max_time = int(result[0])
                        if api_time_key <= db_max_time:
                            print(f"   zzz Dữ liệu cũ (DB mới nhất: {db_max_time}). BỎ QUA.", flush=True)
                            return
        except Exception as e:
            print(f"⚠️ Lỗi check trùng (vẫn tiếp tục): {e}", flush=True)

        # --- BƯỚC 3: TRANSFORM (Chuẩn bị dữ liệu) ---
        print(f"   ✅ Dữ liệu MỚI! Đang xử lý {len(data)} chỉ số...", flush=True)
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

        # --- BƯỚC 4: LOAD (Tự động sửa Dim_Date & Nạp Fact) ---
        print(f"   💾 Đang chuẩn bị nạp {len(df_fact)} dòng...", flush=True)

        unique_dates = sorted(list(set(int(x) for x in df_fact['DateKey'].unique())))
        unique_times = sorted(list(set(int(x) for x in df_fact['TimeKey'].unique())))

        # ---------------------------------------------------------
        # 🔥 FIX QUAN TRỌNG: TỰ ĐỘNG TẠO NGÀY MỚI (AN TOÀN TUYỆT ĐỐI)
        # ---------------------------------------------------------
        try:
            with engine.begin() as conn: # Dùng Transaction để an toàn
                for d_key in unique_dates:
                    # Kiểm tra nhanh xem ngày có chưa
                    exists = conn.execute(text(f'SELECT 1 FROM "Dim_Date" WHERE "DateKey" = {d_key}')).fetchone()
                    
                    if not exists:
                        print(f"   ⚠️ Phát hiện ngày mới {d_key}. Đang tạo...", flush=True)
                        
                        d_str = str(d_key)
                        year = int(d_str[:4])
                        month = int(d_str[4:6])
                        day = int(d_str[6:])
                        date_val = f"{year}-{month:02d}-{day:02d}"
                        
                        dt_temp = datetime(year, month, day)
                        day_of_week = dt_temp.strftime('%A')

                        # SỬ DỤNG ON CONFLICT DO NOTHING ĐỂ TRÁNH LỖI DUPLICATE KHI 2 TIẾN TRÌNH CHẠY CÙNG LÚC
                        insert_dim_sql = text(f"""
                            INSERT INTO "Dim_Date" ("DateKey", "FullDate", "Day", "Month", "Year", "DayOfWeek")
                            VALUES ({d_key}, '{date_val}', {day}, {month}, {year}, '{day_of_week}')
                            ON CONFLICT ("DateKey") DO NOTHING
                        """)
                        
                        conn.execute(insert_dim_sql)
                        print(f"   ✅ Đã xử lý Dim_Date cho {d_key}", flush=True)

        except Exception as e_dim:
            # QUAN TRỌNG: Chỉ in lỗi cảnh báo, KHÔNG RETURN. Vẫn để code chạy xuống dưới lưu Fact.
            print(f"⚠️ Cảnh báo Dim_Date (Vẫn tiếp tục nạp Fact): {e_dim}", flush=True)

        # ---------------------------------------------------------
        # NẠP FACT TABLE 
        # ---------------------------------------------------------
        try:
            date_str = ", ".join(str(x) for x in unique_dates)
            time_str = ", ".join(str(x) for x in unique_times)
            loc_key_val = int(loc_db_map.get(LOCATION_ID))

            # Xóa dữ liệu cũ (nếu có) để tránh duplicate key
            sql_clean = f"""
                DELETE FROM "Fact_AirQuality" 
                WHERE "LocationKey" = {loc_key_val} 
                AND "DateKey" IN ({date_str}) 
                AND "TimeKey" IN ({time_str})
            """

            with engine.begin() as conn:
                conn.execute(text(sql_clean))
                df_fact.to_sql('Fact_AirQuality', conn, if_exists='append', index=False)
            
            print("   🎉 THÀNH CÔNG! Dữ liệu đã được cập nhật.", flush=True)
            
        except Exception as e_fact:
            print(f"❌ Lỗi khi nạp Fact_AirQuality: {e_fact}", flush=True)

    except Exception as e:
        print(f"❌ Lỗi hệ thống: {e}", flush=True)
        import traceback
        traceback.print_exc()

# ==============================================================================
# 3. WEB SERVER & SCHEDULER
# ==============================================================================
app = Flask(__name__)

# Khởi tạo Scheduler (chạy ngầm mỗi 10 phút)
scheduler = BackgroundScheduler()
scheduler.add_job(func=run_realtime_job, trigger="interval", minutes=10)
scheduler.start()

@app.route('/')
def index():
    return "🌍 Service RUNNING. API OpenAQ -> Supabase ETL (Fixed V2)."

@app.route('/update')
def manual():
    run_realtime_job()
    return "✅ Triggered manual update."

if __name__ == "__main__":
    print("⚡ Kích hoạt lần quét đầu tiên...", flush=True)
    run_realtime_job()
    
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port, use_reloader=False)
