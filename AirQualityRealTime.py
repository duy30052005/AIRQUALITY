import os
import requests
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from flask import Flask, jsonify, request
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
import sys
import psutil 
import gc
from sklearn.ensemble import GradientBoostingRegressor
import logging

# ==============================================================================
# 🔥 CẤU HÌNH LOGGING (KHẮC PHỤC LỖI MẤT LOG TRÊN RENDER)
# ==============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger()

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
    # pool_pre_ping=True: Tự động kết nối lại nếu DB bị ngắt
    engine = create_engine(DB_URI, pool_pre_ping=True)
except Exception as e:
    logger.error(f"❌ DB Config Error: {e}")

# --- API KEYS & CONSTANTS ---
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
# 2. HELPER FUNCTIONS
# ==============================================================================
def log_resources(tag=""):
    """Ghi log RAM sử dụng"""
    try:
        process = psutil.Process(os.getpid())
        mem_info = process.memory_info().rss / 1024 / 1024 
        logger.info(f"📊 [{tag}] RAM Usage: {mem_info:.2f} MB")
    except:
        pass

def ensure_dim_date(conn, unique_dates):
    """Tự động điền ngày thiếu vào Dim_Date"""
    for d_key in unique_dates:
        try:
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
        except Exception as e:
            logger.warning(f"⚠️ Dim_Date Error ({d_key}): {e}")

# ==============================================================================
# 3. JOB 1: AIR QUALITY ETL (10 Phút/Lần)
# ==============================================================================
def run_air_quality_job():
    log_resources("AQ-Start")
    logger.info("💨 [AIR QUALITY] ETL Started...")
    
    url = f"https://api.openaq.org/v3/locations/{LOCATION_ID_AQ}/latest"
    headers = {"X-API-Key": OPENAQ_API_KEY}
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code != 200: 
            logger.error(f"❌ AQ API Error: {response.status_code}")
            return

        data = response.json().get('results', [])
        if not data: return
        
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

        # Load into DB
        unique_dates = sorted(df_fact['DateKey'].unique())
        unique_times = sorted(df_fact['TimeKey'].unique())
        date_str = ", ".join(str(x) for x in unique_dates)
        time_str = ", ".join(str(x) for x in unique_times)

        with engine.begin() as conn:
            ensure_dim_date(conn, unique_dates)
            # Xóa trùng lặp trước khi insert
            sql_clean = f"""
                DELETE FROM "Fact_AirQuality"
                WHERE "LocationKey" = {loc_key}
                AND "DateKey" IN ({date_str}) AND "TimeKey" IN ({time_str})
            """
            conn.execute(text(sql_clean))
            df_fact.to_sql('Fact_AirQuality', conn, if_exists='append', index=False)
            logger.info(f"   ✅ AQ Saved: {len(df_fact)} rows.")
            
        del df_fact, processed_rows
        gc.collect()

    except Exception as e:
        logger.error(f"❌ AQ Error: {e}")

# ==============================================================================
# 4. JOB 2: WEATHER ETL (60 Phút/Lần)
# ==============================================================================
def run_weather_job():
    logger.info("☀️ [WEATHER] ETL Started...")
    
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
            logger.info(f"   ✅ Weather Saved: {len(fact_df)} rows.")

        del df, fact_df
        gc.collect()

    except Exception as e:
        logger.error(f"❌ Weather Error: {e}")

# ==============================================================================
# 5. JOB 3: ML FORECASTING (60 Phút/Lần) - Production Ready
# ==============================================================================
def run_ml_forecast_job():
    log_resources("ML-Start")
    logger.info(f"🧠 [ML FORECAST] Start Training & Prediction...")

    try:
        # 1. Lấy danh sách khí
        with engine.connect() as conn:
             p_df = pd.read_sql(text('SELECT "ParameterName" FROM "Dim_Parameter"'), conn)
             gas_list = p_df['ParameterName'].unique().tolist()
        
        if not gas_list: gas_list = ['pm25', 'pm10', 'co', 'no2', 'so2', 'o3']

        # 2. Load dữ liệu Train (30 ngày gần nhất)
        logger.info("   📥 Loading 30 days history...")
        sql = text(f"""
            SELECT 
                (d."FullDate" + t."TimeObj"::time) as "Timestamp",
                p."ParameterName",
                aq."Value" as "AirValue",
                w."Temperature", w."Humidity", w."WindSpeed", w."Rain", w."Pressure"
            FROM "Fact_AirQuality" aq
            JOIN "Fact_Weather" w 
                ON aq."DateKey" = w."DateKey" AND aq."TimeKey" = w."TimeKey"
            JOIN "Dim_Date" d ON aq."DateKey" = d."DateKey"
            JOIN "Dim_Time" t ON aq."TimeKey" = t."TimeKey"
            JOIN "Dim_Parameter" p ON aq."ParameterKey" = p."ParameterKey"
            WHERE d."FullDate" >= CURRENT_DATE - INTERVAL '30 days'
            AND aq."LocationKey" = {LOCATION_ID_AQ}
            ORDER BY "Timestamp" ASC
        """)
        
        df_all = pd.read_sql(sql, engine)
        if df_all.empty or len(df_all) < 100: 
            logger.warning("⚠️ Not enough data to train.")
            return

        # 3. Lấy Dự báo Thời tiết Tương lai (để làm input cho dự báo)
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": LAT, "longitude": LON,
            "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m,rain,surface_pressure",
            "timezone": "Asia/Bangkok",
            "forecast_days": 2
        }
        resp = requests.get(url, params=params)
        w_data = resp.json()
        df_future_weather = pd.DataFrame(w_data['hourly'])
        df_future_weather['time'] = pd.to_datetime(df_future_weather['time'])
        df_future_weather = df_future_weather.set_index('time')
        df_future_weather.rename(columns={
            'temperature_2m': 'Temperature', 'relative_humidity_2m': 'Humidity',
            'wind_speed_10m': 'WindSpeed', 'rain': 'Rain', 'surface_pressure': 'Pressure'
        }, inplace=True)

        all_forecast_results = []
        
        # 4. Vòng lặp Training & Predict từng loại khí
        for gas in gas_list:
            df = df_all[df_all['ParameterName'] == gas].copy()
            if len(df) < 48: continue 

            # --- Feature Engineering ---
            df['Timestamp'] = pd.to_datetime(df['Timestamp'])
            df = df.set_index('Timestamp').sort_index()
            df = df.resample('h').mean().interpolate().dropna()

            df['Target'] = df['AirValue']
            df['Target_Lag1'] = df['Target'].shift(1)
            df['Target_Lag2'] = df['Target'].shift(2)
            df['Target_RollMean6'] = df['Target'].rolling(6).mean()
            df['Rain_Lag1'] = df['Rain'].shift(1)
            df['Wind_Lag1'] = df['WindSpeed'].shift(1)
            df['Hour'] = df.index.hour
            df['Hour_Sin'] = np.sin(2 * np.pi * df['Hour']/24)
            df['Hour_Cos'] = np.cos(2 * np.pi * df['Hour']/24)
            
            train_df = df.dropna()
            
            # --- Train Model (Gradient Boosting) ---
            features = ['Target_Lag1', 'Target_Lag2', 'Target_RollMean6', 
                        'Rain_Lag1', 'Wind_Lag1', 'Temperature', 'Humidity', 'Pressure', 
                        'Hour_Sin', 'Hour_Cos']
            
            model = GradientBoostingRegressor(n_estimators=150, learning_rate=0.05, max_depth=4, random_state=42)
            model.fit(train_df[features], train_df['Target'])

            # --- Predict Recursive (24h Future) ---
            last_row = train_df.iloc[-1].copy()
            last_timestamp = train_df.index[-1]
            
            for i in range(1, 25):
                next_time = last_timestamp + timedelta(hours=i)
                
                # Lấy thời tiết tương lai
                if next_time in df_future_weather.index:
                    w_row = df_future_weather.loc[next_time]
                    next_temp, next_hum, next_pres = w_row['Temperature'], w_row['Humidity'], w_row['Pressure']
                    current_rain, current_wind = w_row['Rain'], w_row['WindSpeed']
                else:
                    next_temp, next_hum, next_pres = last_row['Temperature'], last_row['Humidity'], last_row['Pressure']
                    current_rain, current_wind = 0, 0 

                # Input Vector
                input_data = pd.DataFrame([{
                    'Target_Lag1': last_row['Target'],
                    'Target_Lag2': last_row['Target_Lag1'],
                    'Target_RollMean6': last_row['Target_RollMean6'], 
                    'Rain_Lag1': last_row['Rain'],
                    'Wind_Lag1': last_row['WindSpeed'],
                    'Temperature': next_temp, 'Humidity': next_hum, 'Pressure': next_pres,
                    'Hour_Sin': np.sin(2 * np.pi * next_time.hour/24),
                    'Hour_Cos': np.cos(2 * np.pi * next_time.hour/24)
                }])
                
                # Predict
                pred_val = model.predict(input_data)[0]
                pred_val = max(0, pred_val)
                
                all_forecast_results.append({
                    "Timestamp": next_time,
                    "Parameter": gas,
                    "Value": round(pred_val, 2)
                })
                
                # Update Recursive State
                last_row['Target'] = pred_val
                last_row['Target_Lag1'] = input_data.iloc[0]['Target_Lag1']
                last_row['Rain'] = current_rain
                last_row['WindSpeed'] = current_wind


        # 5. Lưu kết quả vào DB (Ghi đè - Latest Wins)
        if all_forecast_results:
            df_res = pd.DataFrame(all_forecast_results)
            
            with engine.connect() as conn:
                 p_df = pd.read_sql(text('SELECT "ParameterName", "ParameterKey" FROM "Dim_Parameter"'), conn)
                 param_map = dict(zip(p_df['ParameterName'], p_df['ParameterKey']))
                 
                 MODEL_KEY_NAME = "GradientBoosting"
                 m_res = conn.execute(text(f"SELECT \"ModelKey\" FROM \"Dim_Model\" WHERE \"ModelName\" = '{MODEL_KEY_NAME}'")).fetchone()
                 if m_res:
                     model_key = m_res[0]
                 else:
                     ins = conn.execute(text(f"INSERT INTO \"Dim_Model\" (\"ModelName\", \"HorizonType\") VALUES ('{MODEL_KEY_NAME}', 'ShortTerm') RETURNING \"ModelKey\""))
                     model_key = ins.fetchone()[0]
                     conn.commit()
                 
                 l_df = pd.read_sql(text(f'SELECT "LocationKey" FROM "Dim_Location" WHERE "LocationID_Source" = {LOCATION_ID_AQ}'), conn)
                 location_key = l_df.iloc[0]['LocationKey'] if not l_df.empty else 1

            df_res['DateKey'] = df_res['Timestamp'].dt.strftime('%Y%m%d').astype(int)
            df_res['TimeKey'] = df_res['Timestamp'].dt.hour * 100 + df_res['Timestamp'].dt.minute
            df_res['LocationKey'] = location_key
            df_res['ModelKey'] = model_key
            df_res['ParameterKey'] = df_res['Parameter'].map(param_map)
            df_res['CreatedDate'] = datetime.now()
            
            df_insert = df_res[['DateKey', 'TimeKey', 'LocationKey', 'ParameterKey', 'ModelKey', 'Value', 'CreatedDate']].dropna()
            
            with engine.begin() as conn:
                ensure_dim_date(conn, df_insert['DateKey'].unique())
                
                # Chiến lược: Xóa dự báo cũ của dải ngày liên quan -> Insert mới
                d_keys = ",".join(map(str, df_insert['DateKey'].unique()))
                if d_keys:
                    conn.execute(text(f'DELETE FROM "Fact_Forecast" WHERE "ModelKey"={model_key} AND "LocationKey"={location_key} AND "DateKey" IN ({d_keys})'))
                
                df_insert.to_sql('Fact_Forecast', conn, if_exists='append', index=False)
            
            logger.info(f"   🎉 ML Forecast: Saved {len(df_insert)} rows to DB.")

    except Exception as e:
        logger.error(f"❌ ML Forecast Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
    
    del df_all, all_forecast_results
    gc.collect()
    log_resources("ML-End")

# ==============================================================================
# 6. API ENDPOINTS & SERVER
# ==============================================================================
app = Flask(__name__)

@app.route('/api/forecast', methods=['GET'])
def get_forecast():
    """API trả về JSON dự báo 24h tới"""
    loc_id = request.args.get('location_id', default=LOCATION_ID_AQ, type=int)
    try:
        with engine.connect() as conn:
            l_res = conn.execute(text(f'SELECT "LocationKey" FROM "Dim_Location" WHERE "LocationID_Source" = {loc_id}')).fetchone()
            if not l_res: return jsonify({"error": "Location not found"}), 404
            loc_key = l_res[0]
            
            sql = text(f"""
                SELECT (d."FullDate" + t."TimeObj"::time) as "timestamp", p."ParameterName", f."Value"
                FROM "Fact_Forecast" f
                JOIN "Dim_Date" d ON f."DateKey" = d."DateKey"
                JOIN "Dim_Time" t ON f."TimeKey" = t."TimeKey"
                JOIN "Dim_Parameter" p ON f."ParameterKey" = p."ParameterKey"
                JOIN "Dim_Model" m ON f."ModelKey" = m."ModelKey"
                WHERE f."LocationKey" = {loc_key} AND m."ModelName" = 'GradientBoosting'
                AND (d."FullDate" + t."TimeObj"::time) >= CURRENT_TIMESTAMP
                ORDER BY "timestamp" ASC
            """)
            result = conn.execute(sql).fetchall()
            data = [{"timestamp": str(r[0]), "parameter": r[1], "value": r[2]} for r in result]
            return jsonify({"status": "success", "count": len(data), "data": data})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/history', methods=['GET'])
def get_history():
    """API trả về JSON lịch sử đo đạc"""
    hours = request.args.get('hours', default=24, type=int)
    try:
        with engine.connect() as conn:
            l_res = conn.execute(text(f'SELECT "LocationKey" FROM "Dim_Location" WHERE "LocationID_Source" = {LOCATION_ID_AQ}')).fetchone()
            loc_key = l_res[0]
            sql = text(f"""
                SELECT (d."FullDate" + t."TimeObj"::time) as "timestamp", p."ParameterName", f."Value"
                FROM "Fact_AirQuality" f
                JOIN "Dim_Date" d ON f."DateKey" = d."DateKey"
                JOIN "Dim_Time" t ON f."TimeKey" = t."TimeKey"
                JOIN "Dim_Parameter" p ON f."ParameterKey" = p."ParameterKey"
                WHERE f."LocationKey" = {loc_key}
                AND (d."FullDate" + t."TimeObj"::time) >= CURRENT_TIMESTAMP - INTERVAL '{hours} hours'
                ORDER BY "timestamp" ASC
            """)
            result = conn.execute(sql).fetchall()
            data = [{"timestamp": str(r[0]), "parameter": r[1], "value": r[2]} for r in result]
            return jsonify({"status": "success", "count": len(data), "data": data})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/')
def index():
    return "🌍 AI Service Running: AQ + Weather + ML Forecast."

# --- SCHEDULER SETUP ---
scheduler = BackgroundScheduler()
scheduler.add_job(func=run_air_quality_job, trigger="interval", minutes=10)
scheduler.add_job(func=run_weather_job, trigger="interval", minutes=60)
scheduler.add_job(func=run_ml_forecast_job, trigger="interval", minutes=60) 
scheduler.start()

if __name__ == "__main__":
    logger.info("⚡ System Starting...")
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port, use_reloader=False)
