import csv
import random
from datetime import datetime

# Define file name
file_name = 'supply_chain_data.csv'

# Define headers for different data types
headers = {
    'delivery': ['supplier_id', 'delivery_timestamp', 'material_delivered', 'quantity', 'delivery_status'],
    'production': ['production_line_id', 'production_timestamp', 'operation_efficiency', 'downtime_minutes'],
    'vehicle': ['vehicle_id', 'vehicle_timestamp', 'latitude', 'longitude', 'environmental_conditions']
}

def delivery_data():
    return [
        random.randint(1000, 9999),
        datetime.now().isoformat(),
        random.choice(['steel', 'plastic', 'rubber', 'aluminum']),
        random.randint(100, 1000),
        random.choice(['on_time', 'delayed', 'advanced'])
    ]

def production_line_data():
    return [
        random.randint(1, 10),
        datetime.now().isoformat(),
        random.uniform(0.7, 1.0),  # 70% to 100% efficiency
        random.randint(0, 120)
    ]

def vehicle_gps_data():
    return [
        random.randint(100, 500),
        datetime.now().isoformat(),
        round(random.uniform(-90, 90), 6),
        round(random.uniform(-180, 180), 6),
        random.choice(['clear', 'rainy', 'snowy', 'foggy'])
    ]

def write_data_to_csv():
    with open(file_name, mode='w', newline='') as file:
        writer = csv.writer(file)
        # Write headers
        writer.writerow(headers['delivery'] + headers['production'] + headers['vehicle'])

        for _ in range(1000):
            # Combine data from all functions
            data = delivery_data() + production_line_data() + vehicle_gps_data()
            writer.writerow(data)

if __name__ == "__main__":
    write_data_to_csv()
