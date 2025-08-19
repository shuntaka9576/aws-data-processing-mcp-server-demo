-- Visualization Ready Queries

-- 1. 時系列チャート用：1時間ごとの全センサー平均値
SELECT 
  date_trunc('hour', CAST(timestamp AS timestamp)) as time_hour,
  ROUND(AVG(temperature), 2) as avg_temperature,
  ROUND(AVG(humidity), 2) as avg_humidity,
  ROUND(AVG(pressure), 2) as avg_pressure
FROM iot_sensor_database.sensor_data
GROUP BY date_trunc('hour', CAST(timestamp AS timestamp))
ORDER BY time_hour;

-- 2. デバイス別比較チャート用：日別平均値
SELECT 
  device_id,
  date_trunc('day', CAST(timestamp AS timestamp)) as day,
  ROUND(AVG(temperature), 2) as avg_temperature,
  ROUND(AVG(humidity), 2) as avg_humidity,
  ROUND(AVG(pressure), 2) as avg_pressure,
  ROUND(AVG(battery), 0) as avg_battery
FROM iot_sensor_database.sensor_data
GROUP BY device_id, date_trunc('day', CAST(timestamp AS timestamp))
ORDER BY day, device_id;

-- 3. ヒートマップ用：時間帯×デバイス別の温度分布
SELECT 
  device_id,
  EXTRACT(HOUR FROM CAST(timestamp AS timestamp)) as hour_of_day,
  ROUND(AVG(temperature), 2) as avg_temperature,
  COUNT(*) as record_count
FROM iot_sensor_database.sensor_data
GROUP BY device_id, EXTRACT(HOUR FROM CAST(timestamp AS timestamp))
ORDER BY device_id, hour_of_day;

-- 4. 地理的分析用：デバイス位置と最新ステータス
WITH latest_data AS (
  SELECT 
    device_id,
    timestamp,
    temperature,
    humidity,
    pressure,
    battery,
    location.lat as latitude,
    location.lon as longitude,
    ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY timestamp DESC) as rn
  FROM iot_sensor_database.sensor_data
)
SELECT 
  device_id,
  timestamp as last_update,
  temperature,
  humidity,
  pressure,
  battery,
  latitude,
  longitude
FROM latest_data
WHERE rn = 1;

-- 5. トレンド分析用：移動平均（12時間）
WITH hourly_avg AS (
  SELECT 
    device_id,
    date_trunc('hour', CAST(timestamp AS timestamp)) as hour,
    AVG(temperature) as hourly_temp
  FROM iot_sensor_database.sensor_data
  GROUP BY device_id, date_trunc('hour', CAST(timestamp AS timestamp))
)
SELECT 
  device_id,
  hour,
  ROUND(hourly_temp, 2) as temperature,
  ROUND(AVG(hourly_temp) OVER (
    PARTITION BY device_id 
    ORDER BY hour 
    ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
  ), 2) as moving_avg_12h
FROM hourly_avg
ORDER BY device_id, hour;

-- 6. 相関分析用：デバイス間の温度相関
SELECT 
  a.device_id as device_a,
  b.device_id as device_b,
  ROUND(CORR(a.temperature, b.temperature), 3) as temperature_correlation,
  COUNT(*) as sample_size
FROM iot_sensor_database.sensor_data a
JOIN iot_sensor_database.sensor_data b 
  ON a.timestamp = b.timestamp 
  AND a.device_id < b.device_id
GROUP BY a.device_id, b.device_id
HAVING COUNT(*) > 100  -- 十分なサンプルサイズがある場合のみ
ORDER BY temperature_correlation DESC;
