from flask import Flask
import paho.mqtt.client as mqtt
import json
from supabase_py import create_client
import uuid
import datetime

app = Flask(__name__)

# Supabase configuration
supabase_url = 'http://13.51.198.7:8000'
supabase_key = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyAgCiAgICAicm9sZSI6ICJhbm9uIiwKICAgICJpc3MiOiAic3VwYWJhc2UtZGVtbyIsCiAgICAiaWF0IjogMTY0MTc2OTIwMCwKICAgICJleHAiOiAxNzk5NTM1NjAwCn0.dc_X5iR_VP_qT0zsiyj_I_OZ2T9FtRU2BBNWN8Bu4GE'

table_name = 'mqtt_data'
supabase = create_client(supabase_url, supabase_key)

# MQTT configuration
mqtt_broker = '3.106.187.158'
mqtt_port = 3000
mqtt_client = mqtt.Client()



topic_sensor_mapping = {}

def fetch_sensor_data():
    query_result = supabase.from_('customer_sensors').select('id', 'sensor_tag', 'system_id').execute()

    if 'error' in query_result:
        print(f"Error fetching sensor data: {query_result['error']}")
    else:
        sensor_data = query_result.get('data', [])
        for sensor in sensor_data:
            sensor_id = sensor.get('id')
            system_id = sensor.get('system_id')
            sensor_tag = sensor.get('sensor_tag')

            # Get system_tag using system_id
            system_query = supabase.from_('Customer_system').select('system_tag').eq('id', system_id).execute()
            system_tag = system_query['data'][0]['system_tag'] if system_query and system_query['data'] else None

            if system_tag:
                # Subscribe to topics dynamically
                for data_type in ['voltage', 'current', 'resistance']:
                    topic = f"{system_tag}/{sensor_tag}/{data_type}"
                    mqtt_client.subscribe(topic)
                    topic_sensor_mapping[topic] = sensor_id

def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT broker with code:", rc)
    fetch_sensor_data()  # Subscribe to dynamic topics on connect

def on_message(client, userdata, message):
    try:
        payload = message.payload.decode("utf-8")
        topic = message.topic

        # Check if the payload is valid JSON
        try:
            json_data = float(payload)  # Convert payload to float (if applicable)
        except ValueError as e:
            print(f"Invalid payload on topic {topic}: {payload}")
            return

        current_date = datetime.date.today().isoformat()
        current_time = datetime.datetime.now().time().isoformat()

        # Get the sensor_id from the mapping based on the topic
        sensor_id = topic_sensor_mapping.get(topic, None)

        if sensor_id is not None:
            value_data = {
                "id": str(uuid.uuid4()),  # Generate a unique ID
                "topic": topic,
                "payload": json_data,
                "date_time": current_date,
                "time": current_time,
                "sensor_id": sensor_id
            }

            print("Inserting data:", value_data)

            response = supabase.table(table_name).insert([value_data]).execute()

            if 'status_code' in response and response['status_code'] == 201:
                print("Data inserted successfully!")
            else:
                print("Error inserting data:", response)
        else:
            print("Sensor ID not found for topic:", topic)

    except Exception as e:
        print("Error processing MQTT message:", str(e))

# Set MQTT client callbacks
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# Connect MQTT client
mqtt_client.connect(mqtt_broker, mqtt_port)
mqtt_client.loop_start()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)