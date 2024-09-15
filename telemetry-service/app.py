from flask import Flask, jsonify, request
from flask_kafka import FlaskKafka
from flask_swagger_ui import get_swaggerui_blueprint
from datetime import datetime

app = Flask(__name__)

# Initialize Kafka producer
kafka_producer = FlaskKafka('kafka', bootstrap_servers='kafka:9092')

# Dummy data to simulate telemetry-service data
telemetry_data = {
    1: [
        {"timestamp": "2023-09-10T10:00:00", "value": 21.5},
        {"timestamp": "2023-09-11T11:00:00", "value": 22.0}
    ],
    2: [
        {"timestamp": "2023-09-10T10:00:00", "value": 50.0},
        {"timestamp": "2023-09-11T11:00:00", "value": 55.0}
    ]
}

# Swagger setup
SWAGGER_URL = '/swagger'
API_URL = '/static/swagger.json'  # Endpoint where the swagger.json file will be hosted
swaggerui_blueprint = get_swaggerui_blueprint(SWAGGER_URL, API_URL)
app.register_blueprint(swaggerui_blueprint, url_prefix=SWAGGER_URL)

# Endpoints

# Get latest telemetry-service for a device
@app.route('/devices/<int:device_id>/telemetry/latest', methods=['GET'])
def get_latest_telemetry(device_id):
    telemetry = telemetry_data.get(device_id)
    if not telemetry:
        return jsonify({"error": "No telemetry-service data found for the device"}), 404

    latest_telemetry = telemetry[-1]  # Assuming the last entry is the latest
    return jsonify(latest_telemetry), 200

# Get telemetry-service history for a device
@app.route('/devices/<int:device_id>/telemetry', methods=['GET'])
def get_telemetry_history(device_id):
    telemetry = telemetry_data.get(device_id)
    if not telemetry:
        return jsonify({"error": "No telemetry-service data found for the device"}), 404

    start_time = request.args.get('start_time')
    end_time = request.args.get('end_time')

    if not start_time or not end_time:
        return jsonify({"error": "Please provide both start_time and end_time in ISO format"}), 400

    # Filter telemetry-service data by the provided time range
    filtered_telemetry = [
        t for t in telemetry
        if start_time <= t["timestamp"] <= end_time
    ]

    return jsonify(filtered_telemetry), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
