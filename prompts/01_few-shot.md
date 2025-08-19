
## 基本情報

以下のAthenaのテーブル情報を元に、センサーごとの平均値を求めるクエリを作成してください。
時刻はJST, UTC両方を表示してください。

## テーブル構造

* データの期間は、2024年1月から607日分です
* ISO 8601形式の文字列を直接timestamp型にキャストできないため、from_iso8601_timestamp関数を使用してください


```sql
CREATE EXTERNAL TABLE `sensor_data`(
  `device_id` string COMMENT 'from deserializer', 
  `timestamp` string COMMENT 'from deserializer', 
  `temperature` double COMMENT 'from deserializer', 
  `humidity` double COMMENT 'from deserializer', 
  `pressure` double COMMENT 'from deserializer', 
  `location` struct<lat:double,lon:double> COMMENT 'from deserializer', 
  `battery` int COMMENT 'from deserializer')
PARTITIONED BY ( 
  `year` string, 
  `month` string, 
  `day` string)
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://iot-sensor-data-622455551446-ap-northeast-1/sensor-data/'
TBLPROPERTIES (
  'classification'='json', 
  'has_encrypted_data'='false', 
  'projection.day.digits'='2', 
  'projection.day.range'='01,31', 
  'projection.day.type'='integer', 
  'projection.enabled'='true', 
  'projection.month.digits'='2', 
  'projection.month.range'='01,12', 
  'projection.month.type'='integer', 
  'projection.year.range'='2020,2030', 
  'projection.year.type'='integer', 
  'storage.location.template'='s3://iot-sensor-data-622455551446-ap-northeast-1/sensor-data/year=${year}/month=${month}/day=${day}/')
```

## 基本的な実行可能なSQL例

```sql
SELECT DISTINCT device_id 
FROM iot_sensor_database.sensor_data
WHERE year = '2025' AND month = '08' AND day = '15'
ORDER BY device_id;
```
