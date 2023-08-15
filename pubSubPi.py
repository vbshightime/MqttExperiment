import time
import random
import paho.mqtt.client as mqtt
import json

broker_address = "192.168.81.100"  # Mosquitto Cloud broker address
broker_port = 1883  # Default MQTT port

topic = "mqtt/rpi"

end_subscribe_time = 0
start_subscribe_time = 0

def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT broker with result code: " + str(rc))
    #client.subscribe(topic)
    #start_time = time.time()
    #while not client.is_subscribed(topic):
    #    time.sleep(0.1)
    #end_time = time.time()
    #subscribe_latency = end_time-start_time
    #print(f"Subscription Latency: {subscribe_latency:.4f} seconds")

def on_message(client, userdata, msg):
    print("qos level ", msg.qos) 
    print("Message received: " + msg.payload.decode())
    if msg.qos == 1:
        overhead = 5
    elif msg.qos == 2:
        overhead = 7
    else:
        overhead = 2
    message_size = len(msg.payload) + overhead
    print("Message length: ", message_size)

def on_subscribe(client, userdata, mid, granted_qos):
    end_subscribe_time = time.time()
    subscribe_latency = end_subscribe_time - start_subscribe_time
    print(f"subscribe Latency: {subscribe_latency: .4f} seconds")

client = mqtt.Client()
#client.tls_set('/home/vbshightime/Documents/mqtt_certs/ca.crt')
client.on_connect = on_connect
client.on_subscribe = on_subscribe
client.on_message = on_message


start_time = time.time()

client.connect(broker_address, broker_port, 60)

client.loop_start()

while not client.is_connected():
    time.sleep(0.1)

end_time = time.time()

connect_latency = end_time - start_time

print(f"connect Latency: {connect_latency: .4f} seconds")

start_subscribe_time = time.time()

client.subscribe(topic)


try:
    while True:
        # Generate a random number as the message payload
        message = {"payload":"Hello,This message is from publisher client"}
        payload_str = json.dumps(message)
        print("Message Size: ", len(payload_str.encode('utf-8')))
        # Publish the message to the topic
        start_time = time.time()
        while not client.publish(topic, payload_str).rc == mqtt.MQTT_ERR_SUCCESS:
            time.sleep(0.1)
        end_time = time.time()
        publish_latency = end_time - start_time
        print(f"Publish Latency: {publish_latency:.4f} seconds")
        print("Published message: " + payload_str)
        #print("message size after publish: ", pyaload_size)
        time.sleep(10)  # Wait for 1 second

except KeyboardInterrupt:
    # Stop the MQTT loop and disconnect from the broker
    client.loop_stop()
    client.disconnect()
