import boto3
import json
import csv
import os
from botocore.exceptions import ClientError

timestream_client = boto3.client("timestream-write")

DATABASE_NAME = "X_Timestream_DB"
TABLE_NAME = "X_table_DB"
#CSV_FILE_PATH = "/var/task/AnagraficaApparati.csv"  

# def load_csv_mapping(csv_path):
#     mapping = {}
#     try:
#         with open(csv_path, mode="r", encoding="utf-8") as file:
#             reader = csv.DictReader(file, delimiter=";")  
#             for row in reader:
#                 kerberos = row["Kerberos"]
#                 sonda = row["Sonda"]
#                 key = f"{kerberos}-{sonda}"
#                 mapping[key] = {
#                     "IDApparato": row["IDApparato"],
#                     "NomeApparato": row["NomeApparato"],
#                     "ModelloApparato": row["ModelloApparato"],
#                     "ClienteApparato": row["ClienteApparato"],
#                     "Linea": row["Linea"],
#                     "Rack": row["Rack"],
#                     "Fila": row["Fila"],
#                     "Sala": row["Sala"],
#                     "Piano": row["Piano"]
#                 }
#     except Exception as e:
#         print(f"Errore nel caricamento del CSV: {e}")
#     return mapping

# MAPPING = load_csv_mapping(CSV_FILE_PATH)

def lambda_handler(event, context):
    records = []
    
    try:
        payload = json.loads(event.get("body", "{}")) if "body" in event else event
        print("Payload ricevuto:", payload)
        
        if "data" not in payload:
            raise ValueError("Il campo 'data' è mancante nel payload.")
        
        gateway_id = payload.get("gatewayId", "UNKNOWN")
        tags = payload["data"].get("tags", [])
        
        if not tags:
            raise ValueError("Il campo 'tags' è mancante o vuoto nel payload.")
        
        print(f"Numero di tag ricevuti: {len(tags)}")

        for tag in tags:
            if "validity_flag" not in tag or tag["validity_flag"] != "true":
                continue  

            if "registryCode" not in tag:
                print(f"Tag mancante di 'registryCode': {tag}")
                continue  

            registry_code = tag["registryCode"]
            # kerberos, sonda = registry_code.split("-") if "-" in registry_code else (registry_code, "")
            # csv_key = f"{kerberos}-{sonda}"
            # metadata = MAPPING.get(csv_key, {})
            record = {
                "Dimensions": [
                    {"Name": "gateway_id", "Value": gateway_id},
                    {"Name": "registryCode", "Value": registry_code}
                # ] + [
                #     {"Name": k, "Value": v} for k, v in metadata.items() if v 
                ],
                "MeasureName": tag.get("measureName", "UNKNOWN"),
                "MeasureValue": str(tag.get("value", 0)),
                "MeasureValueType": "DOUBLE",
                "Time": tag.get("timestamp"),
                "TimeUnit": "MILLISECONDS",
            }
            print("Record creato:", record)
            records.append(record)

        if records:
            try:
                response = timestream_client.write_records(
                    DatabaseName=DATABASE_NAME,
                    TableName=TABLE_NAME,
                    Records=records
                )
                print("Scrittura riuscita:", response)
                
                if 'RejectedRecords' in response:
                    print("Attenzione: record rifiutati")
            except ClientError as e:
                print("Errore nel write_records:", str(e))

    except ValueError as e:
        print(f"Errore nel payload: {e}")
    except Exception as e:
        print(f"Errore generico: {e}")

    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Dati elaborati con successo"})
    }
