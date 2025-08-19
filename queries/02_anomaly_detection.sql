-- Anomaly Detection Queries

-- 1. 温度異常値の検出（平均±2標準偏差を超える値）
WITH temp_stats AS (
  SELECT 
    device_id,
    AVG(temperature) as avg_temp,
    STDDEV(temperature) as stddev_temp
  FROM iot_sensor_database.sensor_data
  GROUP BY device_id
)
SELECT 
  s.device_id,
  s.timestamp,
  s.temperature,
  ts.avg_temp,
  ts.stddev_temp,
  CASE 
    WHEN s.temperature > (ts.avg_temp + 2 * ts.stddev_temp) THEN 'HIGH_ANOMALY'
    WHEN s.temperature < (ts.avg_temp - 2 * ts.stddev_temp) THEN 'LOW_ANOMALY'
    ELSE 'NORMAL'
  END as anomaly_type
FROM iot_sensor_database.sensor_data s
JOIN temp_stats ts ON s.device_id = ts.device_id
WHERE 
  s.temperature > (ts.avg_temp + 2 * ts.stddev_temp) OR
  s.temperature < (ts.avg_temp - 2 * ts.stddev_temp)
ORDER BY s.timestamp;

-- 2. バッテリー残量警告（20%以下）
SELECT 
  device_id,
  timestamp,
  battery,
  location.lat as latitude,
  location.lon as longitude
FROM iot_sensor_database.sensor_data
WHERE battery <= 20
ORDER BY battery ASC, timestamp DESC;

-- 3. 連続する異常値の検出
WITH anomalies AS (
  SELECT 
    device_id,
    timestamp,
    temperature,
    LAG(temperature) OVER (PARTITION BY device_id ORDER BY timestamp) as prev_temp,
    LEAD(temperature) OVER (PARTITION BY device_id ORDER BY timestamp) as next_temp
  FROM iot_sensor_database.sensor_data
)
SELECT 
  device_id,
  timestamp,
  temperature,
  prev_temp,
  next_temp
FROM anomalies
WHERE 
  ABS(temperature - prev_temp) > 5 AND 
  ABS(temperature - next_temp) > 5
ORDER BY device_id, timestamp;

-- 4. デバイス故障の可能性（データ送信間隔が異常）
WITH time_gaps AS (
  SELECT 
    device_id,
    timestamp,
    LAG(timestamp) OVER (PARTITION BY device_id ORDER BY timestamp) as prev_timestamp,
    EXTRACT(EPOCH FROM (CAST(timestamp AS timestamp) - CAST(LAG(timestamp) OVER (PARTITION BY device_id ORDER BY timestamp) AS timestamp))) / 60 as gap_minutes
  FROM iot_sensor_database.sensor_data
)
SELECT 
  device_id,
  timestamp,
  prev_timestamp,
  gap_minutes
FROM time_gaps
WHERE gap_minutes > 15  -- 15分以上のギャップ
ORDER BY gap_minutes DESC;
