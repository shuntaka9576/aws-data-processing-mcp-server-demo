import { mkdirSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { addMinutes, format } from 'date-fns';
import type { SensorConfig, SensorData } from './types';

class IoTDataGenerator {
  private sensorConfigs: SensorConfig[] = [
    {
      device_id: 'sensor_001',
      location: { lat: 35.6762, lon: 139.6503 }, // 東京駅
      temperatureRange: [18, 28],
      humidityRange: [40, 70],
      pressureRange: [1010, 1020],
      batteryDrainRate: 0.1,
    },
    {
      device_id: 'sensor_002',
      location: { lat: 35.6895, lon: 139.6917 }, // 新宿駅
      temperatureRange: [19, 29],
      humidityRange: [35, 65],
      pressureRange: [1008, 1018],
      batteryDrainRate: 0.15,
    },
    {
      device_id: 'sensor_003',
      location: { lat: 35.709, lon: 139.7319 }, // 池袋駅
      temperatureRange: [17, 27],
      humidityRange: [45, 75],
      pressureRange: [1012, 1022],
      batteryDrainRate: 0.08,
    },
    {
      device_id: 'sensor_004',
      location: { lat: 35.658, lon: 139.7016 }, // 恵比寿駅
      temperatureRange: [20, 30],
      humidityRange: [38, 68],
      pressureRange: [1009, 1019],
      batteryDrainRate: 0.12,
    },
    {
      device_id: 'sensor_005',
      location: { lat: 35.6284, lon: 139.7364 }, // 品川駅
      temperatureRange: [19, 29],
      humidityRange: [42, 72],
      pressureRange: [1011, 1021],
      batteryDrainRate: 0.09,
    },
  ];

  private random(min: number, max: number): number {
    return Math.random() * (max - min) + min;
  }

  private generateAnomalousValue(
    normalValue: number,
    anomalyProbability = 0.02
  ): number {
    if (Math.random() < anomalyProbability) {
      // 異常値: 正常値の±30-50%
      const factor = this.random(1.3, 1.5);
      return Math.random() > 0.5 ? normalValue * factor : normalValue / factor;
    }
    return normalValue;
  }

  private generateSensorData(
    config: SensorConfig,
    timestamp: Date,
    initialBattery = 100
  ): SensorData {
    // 時間による変動を追加（1日周期）
    const hour = timestamp.getHours();
    const timeOfDayFactor = Math.sin((hour * Math.PI) / 12) * 0.3 + 1;

    const baseTemperature = this.random(
      config.temperatureRange[0],
      config.temperatureRange[1]
    );
    const baseHumidity = this.random(
      config.humidityRange[0],
      config.humidityRange[1]
    );
    const basePressure = this.random(
      config.pressureRange[0],
      config.pressureRange[1]
    );

    // 時間による変動を適用
    const temperature = this.generateAnomalousValue(
      baseTemperature * timeOfDayFactor
    );
    const humidity = this.generateAnomalousValue(
      baseHumidity / timeOfDayFactor
    );
    const pressure = this.generateAnomalousValue(
      basePressure + Math.sin((hour * Math.PI) / 12) * 2
    );

    // バッテリー消費（初期値から時間経過で減少）
    const hoursElapsed =
      (timestamp.getTime() - new Date('2024-01-01T00:00:00Z').getTime()) /
      (1000 * 60 * 60);
    const battery = Math.max(
      1,
      Math.floor(initialBattery - hoursElapsed * config.batteryDrainRate)
    );

    return {
      device_id: config.device_id,
      timestamp: timestamp.toISOString(),
      temperature: Math.round(temperature * 10) / 10,
      humidity: Math.round(humidity * 10) / 10,
      pressure: Math.round(pressure * 100) / 100,
      location: config.location,
      battery,
    };
  }

  public generateDataset(
    startDate: Date = new Date('2024-01-01T00:00:00Z'),
    durationDays = 607,
    intervalMinutes = 5,
    dataLossProbability = 0.03
  ): SensorData[] {
    const data: SensorData[] = [];
    const endDate = new Date(
      startDate.getTime() + durationDays * 24 * 60 * 60 * 1000
    );
    let totalExpectedRecords = 0;
    let lostRecords = 0;

    let currentTime = new Date(startDate);

    while (currentTime < endDate) {
      for (const config of this.sensorConfigs) {
        totalExpectedRecords++;

        // ランダムにデータ欠損を発生させる
        if (Math.random() > dataLossProbability) {
          const sensorData = this.generateSensorData(config, currentTime);
          data.push(sensorData);
        } else {
          lostRecords++;
        }
      }
      currentTime = addMinutes(currentTime, intervalMinutes);
    }

    console.log(`Data generation summary:`);
    console.log(`- Expected records: ${totalExpectedRecords}`);
    console.log(`- Generated records: ${data.length}`);
    console.log(`- Lost records: ${lostRecords}`);
    console.log(
      `- Data loss rate: ${((lostRecords / totalExpectedRecords) * 100).toFixed(2)}%`
    );

    return data.sort(
      (a, b) =>
        new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()
    );
  }

  public saveAsJsonL(data: SensorData[], outputPath: string): void {
    const lines = data.map((item) => JSON.stringify(item)).join('\n');
    mkdirSync(outputPath.substring(0, outputPath.lastIndexOf('/')), {
      recursive: true,
    });
    writeFileSync(outputPath, lines, 'utf-8');
    console.log(`Generated ${data.length} records and saved to ${outputPath}`);
  }

  public saveAsPartitionedJsonL(
    data: SensorData[],
    baseOutputPath: string
  ): string[] {
    // 日付別にデータをグループ化
    const dataByDate = new Map<string, SensorData[]>();

    for (const item of data) {
      const date = format(new Date(item.timestamp), 'yyyy-MM-dd');
      if (!dataByDate.has(date)) {
        dataByDate.set(date, []);
      }
      dataByDate.get(date)!.push(item);
    }

    const outputPaths: string[] = [];

    // 日別ファイルとして保存
    for (const [date, dayData] of dataByDate) {
      const year = date.substring(0, 4);
      const month = date.substring(5, 7);
      const day = date.substring(8, 10);

      const partitionDir = join(
        baseOutputPath,
        `year=${year}`,
        `month=${month}`,
        `day=${day}`
      );
      const filePath = join(partitionDir, 'sensor_data.jsonl');

      mkdirSync(partitionDir, { recursive: true });

      const lines = dayData.map((item) => JSON.stringify(item)).join('\n');
      writeFileSync(filePath, lines, 'utf-8');

      outputPaths.push(filePath);
      console.log(
        `Generated ${dayData.length} records for ${date} and saved to ${filePath}`
      );
    }

    return outputPaths;
  }
}

// CLI実行用
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

if (import.meta.url === `file://${process.argv[1]}`) {
  const generator = new IoTDataGenerator();

  console.log('Generating IoT sensor data...');
  const data = generator.generateDataset();

  const baseOutputPath = join(__dirname, '../data');
  const outputPaths = generator.saveAsPartitionedJsonL(data, baseOutputPath);

  console.log(`\nDataset summary:`);
  console.log(`- Total records: ${data.length}`);
  console.log(`- Files created: ${outputPaths.length}`);
  console.log(
    `- Date range: ${data[0].timestamp} to ${data[data.length - 1].timestamp}`
  );
  console.log(
    `- Devices: ${[...new Set(data.map((d) => d.device_id))].join(', ')}`
  );
}

export { IoTDataGenerator };
