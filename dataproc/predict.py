# gsutil cp predict.py gs://mydataproc-pdm/predict.py

import sys
import pickle
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import (
    col, lag, avg, stddev, sum as spark_sum, 
    when, lit, current_timestamp, min as spark_min, 
    max as spark_max, abs as spark_abs, pandas_udf
)
from pyspark.sql.types import DoubleType
import pandas as pd
import gcsfs
import joblib


# Importar funções de cálculo de indicadores do script anterior
def calculate_moving_averages(df):
    window_7 = Window.orderBy("Date").rowsBetween(-6, 0)
    window_14 = Window.orderBy("Date").rowsBetween(-13, 0)
    window_20 = Window.orderBy("Date").rowsBetween(-19, 0)
    window_50 = Window.orderBy("Date").rowsBetween(-49, 0)
    
    df = df.withColumn("MA_7", avg("Close").over(window_7))
    df = df.withColumn("MA_14", avg("Close").over(window_14))
    df = df.withColumn("MA_20", avg("Close").over(window_20))
    df = df.withColumn("MA_50", avg("Close").over(window_50))
    return df

def calculate_rsi(df, period=14):
    df = df.withColumn("price_change", col("Close") - lag("Close", 1).over(Window.orderBy("Date")))
    df = df.withColumn("gain", when(col("price_change") > 0, col("price_change")).otherwise(0))
    df = df.withColumn("loss", when(col("price_change") < 0, spark_abs(col("price_change"))).otherwise(0))
    
    window_rsi = Window.orderBy("Date").rowsBetween(-(period-1), 0)
    df = df.withColumn("avg_gain", avg("gain").over(window_rsi))
    df = df.withColumn("avg_loss", avg("loss").over(window_rsi))
    
    df = df.withColumn("rs", col("avg_gain") / when(col("avg_loss") == 0, 0.0001).otherwise(col("avg_loss")))
    df = df.withColumn("RSI", 100 - (100 / (1 + col("rs"))))
    df = df.drop("price_change", "gain", "loss", "avg_gain", "avg_loss", "rs")
    return df

def calculate_macd(df):
    window_12 = Window.orderBy("Date").rowsBetween(-11, 0)
    window_26 = Window.orderBy("Date").rowsBetween(-25, 0)
    window_9 = Window.orderBy("Date").rowsBetween(-8, 0)
    
    df = df.withColumn("EMA_12", avg("Close").over(window_12))
    df = df.withColumn("EMA_26", avg("Close").over(window_26))
    df = df.withColumn("MACD", col("EMA_12") - col("EMA_26"))
    df = df.withColumn("MACD_Signal", avg("MACD").over(window_9))
    df = df.withColumn("MACD_Histogram", col("MACD") - col("MACD_Signal"))
    df = df.drop("EMA_12", "EMA_26")
    return df

def calculate_bollinger_bands(df, period=20, std_dev=2):
    window = Window.orderBy("Date").rowsBetween(-(period-1), 0)
    df = df.withColumn("BB_Middle", avg("Close").over(window))
    df = df.withColumn("BB_Std", stddev("Close").over(window))
    df = df.withColumn("BB_Upper", col("BB_Middle") + (col("BB_Std") * std_dev))
    df = df.withColumn("BB_Lower", col("BB_Middle") - (col("BB_Std") * std_dev))
    df = df.withColumn("BB_Width", col("BB_Upper") - col("BB_Lower"))
    df = df.drop("BB_Std")
    return df

def calculate_atr(df, period=14):
    df = df.withColumn("prev_close", lag("Close", 1).over(Window.orderBy("Date")))
    df = df.withColumn("tr1", col("High") - col("Low"))
    df = df.withColumn("tr2", spark_abs(col("High") - col("prev_close")))
    df = df.withColumn("tr3", spark_abs(col("Low") - col("prev_close")))
    df = df.withColumn("true_range", 
                       when(col("tr1") >= col("tr2"), 
                            when(col("tr1") >= col("tr3"), col("tr1")).otherwise(col("tr3")))
                       .otherwise(when(col("tr2") >= col("tr3"), col("tr2")).otherwise(col("tr3"))))
    
    window_atr = Window.orderBy("Date").rowsBetween(-(period-1), 0)
    df = df.withColumn("ATR", avg("true_range").over(window_atr))
    df = df.drop("prev_close", "tr1", "tr2", "tr3", "true_range")
    return df

def calculate_keltner_channels(df, period=20, multiplier=2):
    window = Window.orderBy("Date").rowsBetween(-(period-1), 0)
    if "ATR" not in df.columns:
        df = calculate_atr(df, period)
    df = df.withColumn("typical_price", (col("High") + col("Low") + col("Close")) / 3)
    df = df.withColumn("KC_Middle", avg("typical_price").over(window))
    df = df.withColumn("KC_Upper", col("KC_Middle") + (col("ATR") * multiplier))
    df = df.withColumn("KC_Lower", col("KC_Middle") - (col("ATR") * multiplier))
    df = df.drop("typical_price")
    return df

def calculate_stochastic_oscillator(df, period=14, smooth_k=3, smooth_d=3):
    window = Window.orderBy("Date").rowsBetween(-(period-1), 0)
    df = df.withColumn("lowest_low", spark_min("Low").over(window))
    df = df.withColumn("highest_high", spark_max("High").over(window))
    df = df.withColumn("stoch_k_raw", 
                       100 * ((col("Close") - col("lowest_low")) / 
                              when((col("highest_high") - col("lowest_low")) == 0, 0.0001)
                              .otherwise(col("highest_high") - col("lowest_low"))))
    
    window_smooth_k = Window.orderBy("Date").rowsBetween(-(smooth_k-1), 0)
    df = df.withColumn("Stoch_K", avg("stoch_k_raw").over(window_smooth_k))
    
    window_smooth_d = Window.orderBy("Date").rowsBetween(-(smooth_d-1), 0)
    df = df.withColumn("Stoch_D", avg("Stoch_K").over(window_smooth_d))
    df = df.drop("lowest_low", "highest_high", "stoch_k_raw")
    return df

def load_model_from_gcs(spark, model_path):
    """
    Carrega o modelo .pkl do GCS
    """
    print(f"Carregando modelo de: {model_path}")
    
    # Ler o arquivo do GCS como bytes
    sc = spark.sparkContext
    file_content = sc.wholeTextFiles(model_path).collect()
    
    # Alternativa: usar gsutil ou boto3
    # Por simplicidade, assumindo que o arquivo está localmente ou montado
    # Em produção, você pode fazer download via gsutil antes
    
    fs = gcsfs.GCSFileSystem()
    with fs.open(model_path, 'rb') as f:
        model = joblib.load(f)
    print("✅ Modelo carregado com sucesso!")
    return model


def make_predictions(df_pandas, model):
    """
    Faz predições usando o modelo carregado
    """
    # Features esperadas pelo modelo
    feature_columns = [
        'Open', 'High', 'Low', 'Close', 'Volume',
        'MA_7', 'MA_14', 'MA_20', 'MA_50',
        'RSI',
        'MACD', 'MACD_Signal', 'MACD_Histogram',
        'BB_Middle', 'BB_Upper', 'BB_Lower', 'BB_Width',
        'ATR',
        'KC_Middle', 'KC_Upper', 'KC_Lower',
        'Stoch_K', 'Stoch_D'
    ]
    
    # Verificar se todas as features existem
    available_features = [col for col in feature_columns if col in df_pandas.columns]
    
    # Fazer predições
    X = df_pandas[available_features]
    predictions = model.predict(X)
    
    return predictions

def main():
    if len(sys.argv) != 4:
        print("Uso: predict.py <caminho_input_gcs> <caminho_modelo_gcs> <caminho_output_gcs>")
        print("Exemplo: predict.py gs://bucket/data.csv gs://bucket/model.pkl gs://bucket/output/")
        sys.exit(-1)
        
    input_path = sys.argv[1]
    model_path = sys.argv[2]
    output_path = sys.argv[3]
    
    # Iniciar sessão Spark
    spark = SparkSession.builder.appName("StockPricePrediction").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print(f"Lendo dados de: {input_path}")
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    df = df.orderBy("Date")
    
    print("Calculando indicadores técnicos...")
    df = calculate_moving_averages(df)
    df = calculate_rsi(df)
    df = calculate_macd(df)
    df = calculate_bollinger_bands(df)
    df = calculate_atr(df)
    df = calculate_keltner_channels(df)
    df = calculate_stochastic_oscillator(df)
    
    # Remover linhas com NaN
    df = df.dropna()
    
    print(f"Total de linhas após processamento: {df.count()}")
    
    # Carregar modelo
    model = load_model_from_gcs(spark, model_path)
    
    # Converter para Pandas para predição (para datasets pequenos)
    # Para datasets grandes, considere usar pandas_udf
    print("Fazendo predições...")
    df_pandas = df.toPandas()
    predictions = make_predictions(df_pandas, model)
    
    # Adicionar predições ao DataFrame
    df_pandas['Predicted_Next_Day_Price'] = predictions
    df_pandas['Prediction_Date'] = pd.Timestamp.now()
    
    # Converter de volta para Spark DataFrame
    df_result = spark.createDataFrame(df_pandas)
    
    # Adicionar metadados
    df_result = df_result.withColumn("DataTransformacao", current_timestamp())
    df_result = df_result.withColumn("Fonte", lit("RandomForestPrediction"))
    
    print(f"Predições realizadas: {len(predictions)}")
    print(f"\nExemplo de predições:")
    print(df_pandas[['Date', 'Close', 'Predicted_Next_Day_Price']].tail(5))
    
    # Salvar resultado
    output_path = output_path.replace("/raw", "")  # Remove "/raw"
    df_result.coalesce(1).write.mode("overwrite").csv(output_path, header=True)
    
    print(f"\n✅ Predições salvas em: {output_path}")
    
    spark.stop()

if __name__ == "__main__":
    main()
