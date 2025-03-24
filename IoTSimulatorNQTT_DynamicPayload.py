import paho.mqtt.client as mqtt
import ssl
import time
import json
import random

AWS_IOT_ENDPOINT = "xxxx"
PORT = 8883
TOPIC = "xx/xx/xx"
CERTIFICATE_PATH = "certificate.pem.crt"
PRIVATE_KEY_PATH = "private.pem.key"
ROOT_CA_PATH = "AmazonRootCA1.pem"

def read_payload_from_file(filename):
    """Legge il payload dal file JSON."""
    with open(filename, "r") as file:
        return json.load(file)

def get_current_timestamp():
    return int(time.time() * 1000) 

def update_telemetry_values(payload):
    for measure in payload["data"].get("tags", []):  
        measure["timestamp"] = str(get_current_timestamp())
        measure_name = measure.get("measureName")
        if measure_name == "Voltage":
            measure["value"] = str(round(random.uniform(48, 55), 2))  
        elif measure_name == "Current":
            measure["value"] = str(round(random.uniform(20, 80), 2))  
        elif measure_name == "Power":
            measure["value"] = str(round(random.uniform(20, 80), 2)) 
        elif "Energy" in measure_name:  
            measure["value"] = str(round(random.uniform(20, 80), 2)) 
    payload["timestamp"] = str(get_current_timestamp())
    return payload

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connessione al broker riuscita.")
    else:
        print(f"Connessione al broker fallita. Codice errore: {rc}")

def on_publish(client, userdata, mid):
    print(f"Messaggio pubblicato con ID: {mid}")

def on_disconnect(client, userdata, rc):
    if rc == 0:
        print("Disconnessione dal broker avvenuta con successo.")
    else:
        print(f"Errore di disconnessione: {rc}")

client = mqtt.Client(client_id="Simulator_Fib_Thing")
client.on_connect = on_connect
client.on_publish = on_publish
client.on_disconnect = on_disconnect

client.tls_set(
    ca_certs=ROOT_CA_PATH,
    certfile=CERTIFICATE_PATH,
    keyfile=PRIVATE_KEY_PATH,
    tls_version=ssl.PROTOCOL_TLSv1_2
)

print("Connessione al broker MQTT...")
client.connect(AWS_IOT_ENDPOINT, PORT, keepalive=60)
client.loop_start()
time.sleep(2)

try:
    while True:
        payload = read_payload_from_file("FibTelemetry.json")  
        updated_payload = update_telemetry_values(payload) 
        json_payload = json.dumps(updated_payload)
        print(json_payload)
        result = client.publish(TOPIC, json_payload, qos=1)  
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            print("Messaggio inviato correttamente.")
        else:
            print(f"Errore nella pubblicazione: {result.rc}")    
        time.sleep(60)  

except KeyboardInterrupt:
    print("Interruzione manuale, chiusura connessione...")
    client.loop_stop()
    client.disconnect()
