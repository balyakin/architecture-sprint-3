from flask import Flask, jsonify, abort
from flask_swagger_ui import get_swaggerui_blueprint
from kafka import KafkaProducer, KafkaConsumer
import json
from datetime import datetime

app = Flask(__name__)

# Dummy telemetry data (in-memory "database")
telemetry_data = {
    "device-1": [
        {"timestamp": "2024-09-18T12:00:00Z", "data": {"temperature": 22.5, "humidity": 40}},
        {"timestamp": "2024-09-18T13:00:00Z", "data": {"temperature": 23.0, "humidity": 42}}
    ],
    "device-2": [
        {"timestamp": "2024-09-18T12:00:00Z", "data": {"temperature": 20.0, "humidity": 35}}
    ]
}

# Kafka configuration
KAFKA_TOPIC = "telemetry-data"
producer = KafkaProducer(bootstrap_servers="kafka:9092", value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Swagger configuration
SWAGGER_URL = '/doc'
API_URL = '/static/swagger.json'
swaggerui_blueprint = get_swaggerui_blueprint(SWAGGER_URL, API_URL, config={'app_name': "Telemetry Service"})
app.register_blueprint(swaggerui_blueprint, url_prefix=SWAGGER_URL)

# Get latest telemetry data
@app.route('/telemetry/<device_id>/telemetry/latest', methods=['GET'])
def get_latest_telemetry(device_id):
    if device_id not in telemetry_data:
        abort(404, description="Device not found")
    latest_data = telemetry_data[device_id][-1]  # Get the latest telemetry
    return jsonify(latest_data)

# Get historical telemetry data
@app.route('/telemetry/<device_id>/telemetry', methods=['GET'])
def get_historical_telemetry(device_id):
    if device_id not in telemetry_data:
        abort(404, description="Device not found")
    return jsonify(telemetry_data[device_id])

# Run Kafka consumer for telemetry data
def kafka_consumer():
    consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers="kafka:9092", auto_offset_reset='earliest', value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    for message in consumer:
        print(f"Received telemetry from Kafka: {message.value}")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)
