export interface Location {
  lat: number;
  lon: number;
}

export interface SensorData {
  device_id: string;
  timestamp: string;
  temperature: number;
  humidity: number;
  pressure: number;
  location: Location;
  battery: number;
}

export interface SensorConfig {
  device_id: string;
  location: Location;
  temperatureRange: [number, number];
  humidityRange: [number, number];
  pressureRange: [number, number];
  batteryDrainRate: number;
}
