# PDM - trabalho final
## Arquitetura de Predição de Ações no GCP

### Visão Geral

Este documento descreve a arquitetura de um sistema serverless no Google Cloud Platform (GCP) para processamento e predição de dados de ações do mercado financeiro. O sistema utiliza uma abordagem orientada a eventos para automatizar o pipeline de dados desde a ingestão até a predição.

### Componentes da Arquitetura

#### 1. Camada de API (FastAPI)
- **Função**: Ponto de entrada para requisições do cliente
- **Tecnologia**: FastAPI
- **Responsabilidades**:
  - Receber requisições HTTP com o ticker da ação desejada
  - Validar os parâmetros de entrada
  - Orquestrar o processo de coleta de dados

#### 2. Coleta de Dados (yfinance)
- **Função**: Obtenção de dados históricos de ações
- **Tecnologia**: biblioteca yfinance
- **Responsabilidades**:
  - Buscar dados históricos da ação especificada
  - Gerar arquivo CSV com os dados coletados
  - Fazer upload do arquivo para o Cloud Storage

#### 3. Armazenamento Raw (Cloud Storage)
- **Função**: Data Lake - camada raw
- **Localização**: Bucket GCS com pasta `/raw`
- **Responsabilidades**:
  - Armazenar dados brutos em formato CSV
  - Servir como trigger para o pipeline de processamento

#### 4. Sistema de Mensageria (Cloud Pub/Sub)
- **Função**: Gerenciamento de eventos
- **Tecnologia**: Google Cloud Pub/Sub
- **Responsabilidades**:
  - Monitorar eventos de criação de arquivos no bucket `/raw`
  - Publicar mensagens quando novos CSVs são detectados
  - Garantir entrega confiável de notificações

#### 5. Orquestrador (Cloud Function - trigger_dataproc_job)
- **Função**: Iniciador do processamento
- **Tecnologia**: Google Cloud Functions
- **Responsabilidades**:
  - Subscrever tópico do Pub/Sub
  - Receber notificações de novos arquivos
  - Disparar job no Dataproc com os parâmetros necessários

#### 6. Processamento Distribuído (Dataproc)
- **Função**: Transformação e predição de dados
- **Tecnologia**: Google Cloud Dataproc (Apache Spark)
- **Script**: `predict.py`
- **Responsabilidades**:
  - Ler CSV do Cloud Storage
  - Processar dados usando PySpark
  - Aplicar feature engineering
  - Carregar modelo de ML (arquivo .pkl)
  - Gerar predições
  - Salvar resultados processados

#### 7. Modelo de Machine Learning
- **Função**: Inferência de predições
- **Formato**: arquivo .pkl (Python pickle)
- **Localização**: Cloud Storage
- **Responsabilidades**:
  - Fornecer modelo pré-treinado para predições
  - Ser carregado dinamicamente pelo script de processamento

### Fluxo de Dados

```
┌─────────────┐
│   Cliente   │
└──────┬──────┘
       │ HTTP Request (ticker)
       ▼
┌─────────────┐
│   FastAPI   │
└──────┬──────┘
       │ yfinance.download()
       ▼
┌─────────────────────┐
│ Cloud Storage (/raw)│◄───┐
└──────┬──────────────┘    │
       │ Object Created    │
       ▼                   │
┌─────────────┐            │
│  Pub/Sub    │            │
└──────┬──────┘            │
       │ Message           │
       ▼                   │
┌──────────────────────┐   │
│ Cloud Function       │   │
│ (trigger_dataproc)   │   │
└──────┬───────────────┘   │
       │ Submit Job        │
       ▼                   │
┌──────────────────────┐   │
│    Dataproc          │   │
│   (predict.py)       │   │
│  - PySpark           │───┘ Read CSV
│  - Feature Eng       │
│  - Load .pkl model   │◄─── Model Storage
│  - Predictions       │
└──────┬───────────────┘
       │ Write Results
       ▼
┌─────────────────────┐
│ Cloud Storage       │
│ (processed data)    │
└─────────────────────┘
```


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
