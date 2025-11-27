# PDM - trabalho final

## Deploy do pipeline
1. No /data, rodar: <br>
 ``` gcloud run deploy pdm-projeto-final --source . --region us-central1 --allow-unauthenticated  --set-env-vars DATA_BUCKET=mydata-pdm ```
2. Caso não tenha um modelo, suba o dataproc/transform.py no GCP Storage, baixe os arquvios, treine-o localmente, e suba o modelo no GCP Storage. Com o modelo já no GCP, suba o dataproc/predict.py.
<br>Para subir o transform.py:
<br>``` gsutil cp predict.py gs://mydataproc-pdm/predict.py  ```
<br>Para subir o modelo:
<br>``` gsutil cp stock_rf_model.pkl gs://mydataproc-pdm/models/ ```
<br>Para subir o predict.py:
<br>``` gsutil cp predict.py gs://mydataproc-pdm/predict.py ```
3. No /orq, subir o orquestrador:
<br>``` gcloud functions deploy trigger-pyspark-job `
    --gen2 `
    --runtime python310 `
    --region us-central1 `
    --source . `
    --entry-point trigger_dataproc_job `
    --trigger-topic=mygcs-file-uploads `
    --memory 256Mi ``` 
