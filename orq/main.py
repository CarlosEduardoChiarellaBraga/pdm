import functions_framework
import re
import time
from google.cloud import dataproc_v1
import base64
import json

PROJECT_ID = "pdm-projeto-final"       
REGION = "us-central1"                   
SCRIPT_BUCKET="mydataproc-pdm"
SCRIPT_NAME = "predict.py"             

@functions_framework.cloud_event
def trigger_dataproc_job(cloud_event):
    """
    Função acionada quando um arquivo é finalizado no GCS.
    Cria um Batch (Serverless) no Dataproc.
    """
    pubsub_message = base64.b64decode(cloud_event.data["message"]["data"])
    notification_data = json.loads(pubsub_message)
    
    event_bucket = notification_data["bucket"]
    event_file = notification_data["name"]
    
    # Ignora se for arquivo temporário ou pasta de saída para evitar loop infinito
    if "_output" in event_file or "temp" in event_file:
        print(f"Ignorando arquivo: {event_file}")
        return

    print(f"Arquivo detectado: gs://{event_bucket}/{event_file}")

    # 2. Definir caminhos de entrada e saída
    input_uri = f"gs://{event_bucket}/{event_file}"
    # Cria uma pasta de saída baseada no nome do arquivo
    output_uri = f"gs://{event_bucket}/processed/{event_file.replace('.csv', '')}_output"

    # 3. Configurar o Batch (Job Serverless)
    batch_client = dataproc_v1.BatchControllerClient(
        client_options={"api_endpoint": f"{REGION}-dataproc.googleapis.com:443"}
    )

    # Gerar um ID único para o batch (obrigatório: letras minusculas, numeros, hifens)
    # Ex: batch-petr4-16788899
    safe_name = re.sub(r'[^a-z0-9]', '-', event_file.lower().replace('.csv', ''))
    batch_id = f"job-{safe_name}-{int(time.time())}"

    batch = dataproc_v1.Batch()
    batch.pyspark_batch.main_python_file_uri = f"gs://{SCRIPT_BUCKET}/{SCRIPT_NAME}"
    # batch.pyspark_batch.args = [input_uri, output_uri] # -> transform.py 
    batch.pyspark_batch.args = [input_uri, "gs://mydataproc-pdm/models/stock_rf_model.pkl", output_uri] # -> predict.py
    # Opcional: Configurações de performance
    batch.runtime_config.version = "2.2" # Spark 3.5

    # 4. Enviar o Job
    print(f"Enviando Batch ID: {batch_id}...")
    operation = batch_client.create_batch(
        parent=f"projects/{PROJECT_ID}/locations/{REGION}",
        batch=batch,
        batch_id=batch_id
    )

    print(f"Job submetido com sucesso: {operation.operation.name}")



"""
gcloud functions deploy trigger-pyspark-job `
    --gen2 `
    --runtime python310 `
    --region us-central1 `
    --source . `
    --entry-point trigger_dataproc_job `
    --trigger-topic=mygcs-file-uploads `
    --memory 256Mi
"""
