from datetime import datetime
import time
import httpx
import json
from flask import Flask, request, jsonify
from firebase_admin import credentials, messaging
import firebase_admin
import supabase
import uuid

app = Flask(__name__)

# Initialize Firebase
cred = credentials.Certificate('D:\\supabase_mqtt\\ies-67563-firebase-adminsdk-6epw7-4ebad2bfc3.json')
firebase_admin.initialize_app(cred)

# Initialize Supabase
supabase_url = 'http://13.51.198.7:8000'
supabase_key = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyAgCiAgICAicm9sZSI6ICJhbm9uIiwKICAgICJpc3MiOiAic3VwYWJhc2UtZGVtbyIsCiAgICAiaWF0IjogMTY0MTc2OTIwMCwKICAgICJleHAiOiAxNzk5NTM1NjAwCn0.dc_X5iR_VP_qT0zsiyj_I_OZ2T9FtRU2BBNWN8Bu4GE'

supabase_client = supabase.Client(supabase_url, supabase_key)

notifications_table_name = 'notification_table'


def send_firebase_notification(device_id, notification_message):
    message = messaging.Message(
        notification=messaging.Notification(
            title='Threshold Notification',
            body=notification_message
        ),
        token=device_id
    )
    response = messaging.send(message)
    return response

def store_notification(device_id, payload, topic, sensor_id):
    threshold = 3.0  # Define your threshold value here
    message = "Threshold crossed" if payload > threshold else "Threshold not crossed"

    id = str(uuid.uuid4())
    current_date = datetime.now().date().isoformat()
    current_time = datetime.now().time().isoformat()

    try:
        # Fetch all data and find the maximum sequence value
        response = supabase_client.table(notifications_table_name).select('sequence').limit(1).execute()

        if isinstance(response, dict) and 'data' in response:
            data_list = response['data']
            max_sequence = max(data_list, key=lambda x: x.get('sequence', 0)).get('sequence', 0)
        else:
            print('Error fetching max sequence value. Setting sequence to 1.')
            max_sequence = 0

        # Increment the sequence for the new entry
        new_sequence = max_sequence + 1

        data = {
            'id': id,
            'device_id': device_id,
            'payload': payload,
            'topic': topic,
            'date_time': current_date,
            'time': current_time,
            'sensor_id': sensor_id,
            'message': message,
            'flag': 1,
            'sequence': new_sequence  # Include the new sequence value
        }

        print("Inserting data with new sequence:", data)

        # Insert the data with the new sequence value
        response = supabase_client.table(notifications_table_name).insert([data]).execute()

        if isinstance(response, dict) and 'status_code' in response and response['status_code'] == 201:
            print("Data inserted successfully!")
        else:
            error_response = response.json() if hasattr(response, 'json') else str(response)
            print(f'Error inserting data. Response: {error_response}')
            

    except Exception as e:
        print('Error inserting data:', str(e))

def update_flag(notification_id, flag_value):
    try:
        response = supabase_client.table(notifications_table_name).update({'flag': flag_value}).eq('id', notification_id).execute()
        print(response)
        if isinstance(response, dict) and 'status_code' in response and response['status_code'] == 200:
            print("Flag updated successfully!")
        else:
            error_response = response.json() if hasattr(response, 'json') else str(response)
            print(f'Error updating flag. Response: {error_response}')

    except Exception as e:
        print('Error updating flag:', str(e))

def get_device_id():
    try:
        headers = {
            'Authorization': f'Bearer {supabase_key}',
            'Content-Type': 'application/json',
            'apikey': supabase_key
        }

        response = httpx.get(f'{supabase_url}/rest/v1/rpc/get_register_info', headers=headers)

        if response.status_code == 200:
            data = response.json()
            if data and isinstance(data, dict) and 'data' in data:
                device_data = data['data']
                if isinstance(device_data, list) and len(device_data) > 0:
                    return device_data[0].get('device_id')
                else:
                    print('Error: No device_id found in the API response')
                    return None
            else:
                print('Error: Invalid API response format')
                return None
        else:
            print('Error fetching device_id:', response.text)
            return None

    except Exception as e:
        print('Error:', e)
        return None

def fetch_live_data():
    try:
        headers = {
            'Authorization': f'Bearer {supabase_key}',
            'Content-Type': 'application/json',
            'apikey': supabase_key
        }

        response = httpx.get(f'{supabase_url}/rest/v1/rpc/get_all_mqtt_data', headers=headers)

        if response.status_code == 200:
            live_data = response.json()
            return live_data
        else:
            print('Error fetching live data:', response.text)
            return []

    except Exception as e:
        print('Error:', e)
        return []

@app.route('/')
def home():
    return "Flask MQTT Data Processing"

@app.route('/test_process', methods=['POST'])
def test_process():
    device_id = get_device_id()

    if not device_id:
        return "Error fetching device ID", 400

    while True:
        live_data = fetch_live_data()

        if live_data is None:
            print("No live data available")
            time.sleep(10)
            continue

        for data in live_data:
            topic = data.get('topic')
            payload = float(data.get('payload', 0))
            sensor_id = data.get('sensor_id')  # Add this line to fetch sensor_id

            print(f"Received data - Topic: {topic}, Payload: {payload}, Device ID: {device_id},sensor_id:{sensor_id}")

            if topic:
                message = f"Topic: {topic}, Payload: {payload}, Device ID: {device_id},sensor_id:{sensor_id}"
                send_firebase_notification(device_id, message)
                store_notification(device_id, payload, topic, sensor_id)  # Pass sensor_id

        time.sleep(5)

    return json.dumps({"status": "Notifications continuously monitored"})

@app.route('/view_notifications', methods=['POST'])
def view_notifications():
    notification_id = request.json.get('notification_id')
    if notification_id:
        update_flag(notification_id, 0)  # Set flag to 0 when the user views the notification
        return jsonify({"status": f"Flag for notification {notification_id} updated successfully"})
    else:
        return jsonify({"error": "Notification ID not provided"}), 400

if __name__ == "__main__":
    app.run(debug=True)
