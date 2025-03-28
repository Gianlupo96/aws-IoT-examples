import paho.mqtt.client as mqtt
import ssl
import time
import json
import random

AWS_IOT_ENDPOINT = "a10z8hoeefqtlg-ats.iot.eu-central-1.amazonaws.com"
PORT = 8883
TOPIC = "Fibercop/gatewayTest/datiTest/telemetria"
CERTIFICATE_PATH = "simFib-certificate.pem.crt"
PRIVATE_KEY_PATH = "simFib-private.pem.key"
ROOT_CA_PATH = "AmazonRootCA1.pem"

def read_payload_from_file(filename):
    """Legge il payload dal file JSON."""
    with open(filename, "r") as file:
        return json.load(file)

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
        json_payload = json.dumps(payload)
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
 