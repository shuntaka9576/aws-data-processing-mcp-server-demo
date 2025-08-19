-- Basic IoT Sensor Data Queries for Athena

-- 1. 全データの概要
SELECT 
  COUNT(*) as total_records,
  COUNT(DISTINCT device_id) as device_count,
  MIN(timestamp) as earliest_timestamp,
  MAX(timestamp) as latest_timestamp
FROM iot_sensor_database.sensor_data;

-- 2. デバイス別の統計情報
SELECT 
  device_id,
  COUNT(*) as record_count,
  ROUND(AVG(temperature), 2) as avg_temperature,
  ROUND(AVG(humidity), 2) as avg_humidity,
  ROUND(AVG(pressure), 2) as avg_pressure,
  MIN(battery) as min_battery,
  MAX(battery) as max_battery
FROM iot_sensor_database.sensor_data
GROUP BY device_id
ORDER BY device_id;

-- 3. 時間別の平均温度（1時間ごと）
SELECT 
  date_trunc('hour', CAST(timestamp AS timestamp)) as hour,
  ROUND(AVG(temperature), 2) as avg_temperature,
  COUNT(*) as record_count
FROM iot_sensor_database.sensor_data
GROUP BY date_trunc('hour', CAST(timestamp AS timestamp))
ORDER BY hour;

-- 4. 日別の最高・最低温度
SELECT 
  date_trunc('day', CAST(timestamp AS timestamp)) as day,
  ROUND(MIN(temperature), 2) as min_temperature,
  ROUND(MAX(temperature), 2) as max_temperature,
  ROUND(AVG(temperature), 2) as avg_temperature
FROM iot_sensor_database.sensor_data
GROUP BY date_trunc('day', CAST(timestamp AS timestamp))
ORDER BY day;
