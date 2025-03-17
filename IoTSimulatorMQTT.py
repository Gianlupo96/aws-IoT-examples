import paho.mqtt.client as mqtt
import ssl
import time
import json

AWS_IOT_ENDPOINT = "xxxxxx"
PORT = 8883
TOPIC = "xx/xx/xx"

CERTIFICATE_PATH = "certificate.pem.crt"
PRIVATE_KEY_PATH = "private.pem.key"
ROOT_CA_PATH = "AmazonRootCA1.pem"


def read_payload_from_file(filename):
    with open(filename, "r") as file:
        return json.load(file)

payload = read_payload_from_file("payload.json")

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connessione riuscita!")
    else:
        print(f"Connessione fallita con codice {rc}")


def on_disconnect(client, userdata, rc):
    print("Disconnesso dal broker AWS IoT")


client = mqtt.Client()
client.tls_set(
    ca_certs=ROOT_CA_PATH,
    certfile=CERTIFICATE_PATH,
    keyfile=PRIVATE_KEY_PATH,
    tls_version=ssl.PROTOCOL_TLSv1_2
)
client.on_connect = on_connect
client.on_disconnect = on_disconnect

print("Tentativo di connessione...")
client.connect(AWS_IOT_ENDPOINT, PORT, keepalive=60)
client.loop_start()

try:
    while True:
        json_payload = json.dumps(payload)
        client.publish(TOPIC, json_payload)
        print(f"Messaggio inviato: {json_payload}")
        time.sleep(60)  
except KeyboardInterrupt:
    print("Interruzione manuale, chiusura connessione...")
    client.loop_stop()
    client.disconnect()