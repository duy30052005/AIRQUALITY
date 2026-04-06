-- ==============================================================================
-- HANOI AIR QUALITY DATA WAREHOUSE SCHEMA (POSTGRESQL)
-- Kiến trúc: Galaxy Schema
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 1. TẠO CÁC BẢNG CHIỀU (DIMENSION TABLES)
-- ------------------------------------------------------------------------------

CREATE TABLE "Dim_Date" (
    "DateKey" INT PRIMARY KEY,
    "FullDate" DATE NOT NULL,
    "Year" INT,
    "Month" INT,
    "Day" INT,
    "DayOfWeek" VARCHAR(20)
);

CREATE TABLE "Dim_Time" (
    "TimeKey" INT PRIMARY KEY,
    "TimeObj" TIME,
    "Hour" INT,
    "Minute" INT,
    "TimeStr" VARCHAR(10)
);

CREATE TABLE "Dim_Location" (
    "LocationKey" SERIAL PRIMARY KEY,
    "LocationID_Source" VARCHAR(50),
    "LocationName" VARCHAR(255),
    "Latitude" FLOAT,
    "Longitude" FLOAT,
    "CountryISO" VARCHAR(10)
);

CREATE TABLE "Dim_Parameter" (
    "ParameterKey" SERIAL PRIMARY KEY,
    "ParameterName" VARCHAR(50) NOT NULL,
    "Unit" VARCHAR(50)
);

CREATE TABLE "Dim_Model" (
    "ModelKey" SERIAL PRIMARY KEY,
    "ModelName" VARCHAR(100),
    "Version" VARCHAR(50),
    "HorizonType" VARCHAR(50),
    "Description" TEXT
);

-- ------------------------------------------------------------------------------
-- 2. TẠO CÁC BẢNG SỰ KIỆN (FACT TABLES)
-- ------------------------------------------------------------------------------

-- Bảng Fact: Dữ liệu Chất lượng không khí (Real-time từ OpenAQ)
CREATE TABLE "Fact_AirQuality" (
    "DateKey" INT REFERENCES "Dim_Date"("DateKey"),
    "TimeKey" INT REFERENCES "Dim_Time"("TimeKey"),
    "LocationKey" INT REFERENCES "Dim_Location"("LocationKey"),
    "ParameterKey" INT REFERENCES "Dim_Parameter"("ParameterKey"),
    "SourceKey" INT,
    "Value" FLOAT,
    PRIMARY KEY ("DateKey", "TimeKey", "LocationKey", "ParameterKey")
);

-- Bảng Fact: Dữ liệu Thời tiết (Từ Open-Meteo)
CREATE TABLE "Fact_Weather" (
    "WeatherID" SERIAL PRIMARY KEY,
    "DateKey" INT REFERENCES "Dim_Date"("DateKey"),
    "TimeKey" INT REFERENCES "Dim_Time"("TimeKey"),
    "LocationKey" INT REFERENCES "Dim_Location"("LocationKey"),
    "Temperature" FLOAT,
    "Humidity" FLOAT,
    "WindSpeed" FLOAT,
    "Rain" FLOAT,
    "Pressure" FLOAT,
    "CloudCover" FLOAT,
    "CreatedDate" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Bảng Fact: Dữ liệu Dự báo (Kết quả từ Gradient Boosting Regressor)
CREATE TABLE "Fact_Forecast" (
    "ForecastID" SERIAL PRIMARY KEY,
    "DateKey" INT REFERENCES "Dim_Date"("DateKey"),
    "TimeKey" INT REFERENCES "Dim_Time"("TimeKey"),
    "LocationKey" INT REFERENCES "Dim_Location"("LocationKey"),
    "ParameterKey" INT REFERENCES "Dim_Parameter"("ParameterKey"),
    "ModelKey" INT REFERENCES "Dim_Model"("ModelKey"),
    "Value" FLOAT,
    "CreatedDate" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
