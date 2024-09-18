from flask import Flask, jsonify, request, abort
from flask_swagger_ui import get_swaggerui_blueprint
from kafka import KafkaProducer, KafkaConsumer
import json

app = Flask(__name__)

# Dummy device list (in-memory "database")
devices = [
    {"id": "device-1", "name": "Thermostat", "status": "off", "type": "Thermostat", "house_id": "house-1"},
    {"id": "device-2", "name": "Light", "status": "on", "type": "Light", "house_id": "house-1"},
    {"id": "device-3", "name": "Door Lock", "status": "locked", "type": "Lock", "house_id": "house-2"}
]

# Kafka configuration
KAFKA_TOPIC = "device-commands"
producer = KafkaProducer(bootstrap_servers="kafka:9092", value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Swagger configuration
SWAGGER_URL = '/doc'
API_URL = '/static/swagger.json'
swaggerui_blueprint = get_swaggerui_blueprint(SWAGGER_URL, API_URL, config={'app_name': "Devices Service"})
app.register_blueprint(swaggerui_blueprint, url_prefix=SWAGGER_URL)


# Get device info
@app.route('/devices/<device_id>', methods=['GET'])
def get_device(device_id):
    device = next((device for device in devices if device['id'] == device_id), None)
    if device is None:
        abort(404, description="Device not found")
    return jsonify(device)


# Update device status
@app.route('/devices/<device_id>/status', methods=['PUT'])
def update_device_status(device_id):
    device = next((device for device in devices if device['id'] == device_id), None)
    if device is None:
        abort(404, description="Device not found")

    if not request.json or 'status' not in request.json:
        abort(400, description="Invalid request")

    new_status = request.json['status']
    device['status'] = new_status
    return jsonify(device)


# Send command to device
@app.route('/devices/<device_id>/commands', methods=['POST'])
def send_command(device_id):
    device = next((device for device in devices if device['id'] == device_id), None)
    if device is None:
        abort(404, description="Device not found")

    if not request.json or 'command' not in request.json:
        abort(400, description="Invalid command")

    command = request.json['command']
    value = request.json.get('value', None)

    # Send command to Kafka
    command_data = {"device_id": device_id, "command": command, "value": value}
    producer.send(KAFKA_TOPIC, command_data)

    return jsonify({"status": "Command sent", "command": command_data})


# Run Kafka consumer for device commands
def kafka_consumer():
    consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers="kafka:9092", auto_offset_reset='earliest',
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    for message in consumer:
        print(f"Received command from Kafka: {message.value}")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
