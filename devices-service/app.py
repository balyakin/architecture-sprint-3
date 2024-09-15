from flask import Flask, jsonify, request
from flask_kafka import FlaskKafka
from flask_swagger_ui import get_swaggerui_blueprint

app = Flask(__name__)

# Initialize Kafka producer
kafka_producer = FlaskKafka('kafka', bootstrap_servers='kafka:9092')

# Dummy data to simulate device information
devices = {
    1: {"id": 1, "name": "Thermostat", "status": "off", "current_temperature": 20},
    2: {"id": 2, "name": "Light", "status": "on", "brightness": 75}
}

# Swagger setup
SWAGGER_URL = '/doc'
API_URL = '/static/swagger.json'
swaggerui_blueprint = get_swaggerui_blueprint(SWAGGER_URL, API_URL)
app.register_blueprint(swaggerui_blueprint, url_prefix=SWAGGER_URL)

# Endpoints

@app.route('/devices/<int:device_id>', methods=['GET'])
def get_device(device_id):
    device = devices.get(device_id)
    if not device:
        return jsonify({"error": "Device not found"}), 404
    return jsonify(device), 200

@app.route('/devices/<int:device_id>/status', methods=['PUT'])
def update_device_status(device_id):
    device = devices.get(device_id)
    if not device:
        return jsonify({"error": "Device not found"}), 404

    status = request.json.get('status')
    if status not in ['on', 'off']:
        return jsonify({"error": "Invalid status. Must be 'on' or 'off'."}), 400

    device['status'] = status

    kafka_producer.send('device-status-updates', value=f"Device {device_id} status changed to {status}")

    return jsonify({"message": f"Device {device_id} status updated to {status}"}), 200

@app.route('/devices/<int:device_id>/commands', methods=['POST'])
def send_device_command(device_id):
    device = devices.get(device_id)
    if not device:
        return jsonify({"error": "Device not found"}), 404

    command = request.json.get('command')
    if not command:
        return jsonify({"error": "Command not provided"}), 400

    kafka_producer.send('device-commands', value=f"Command sent to device {device_id}: {command}")

    return jsonify({"message": f"Command sent to device {device_id}"}), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
