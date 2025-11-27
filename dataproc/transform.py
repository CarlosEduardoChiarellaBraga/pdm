# não faz as predições, apenas processa

import sys
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import (
    col, lag, avg, stddev, sum as spark_sum, 
    when, lit, current_timestamp, min as spark_min, 
    max as spark_max, abs as spark_abs
)

def calculate_moving_averages(df):
    """Calcula médias móveis de diferentes períodos"""
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
    """Calcula o Relative Strength Index (RSI)"""
    # Calcular a diferença de preço
    df = df.withColumn("price_change", col("Close") - lag("Close", 1).over(Window.orderBy("Date")))
    
    # Separar ganhos e perdas
    df = df.withColumn("gain", when(col("price_change") > 0, col("price_change")).otherwise(0))
    df = df.withColumn("loss", when(col("price_change") < 0, spark_abs(col("price_change"))).otherwise(0))
    
    # Calcular média de ganhos e perdas
    window_rsi = Window.orderBy("Date").rowsBetween(-(period-1), 0)
    df = df.withColumn("avg_gain", avg("gain").over(window_rsi))
    df = df.withColumn("avg_loss", avg("loss").over(window_rsi))
    
    # Calcular RS e RSI
    df = df.withColumn("rs", col("avg_gain") / when(col("avg_loss") == 0, 0.0001).otherwise(col("avg_loss")))
    df = df.withColumn("RSI", 100 - (100 / (1 + col("rs"))))
    
    # Limpar colunas intermediárias
    df = df.drop("price_change", "gain", "loss", "avg_gain", "avg_loss", "rs")
    
    return df

def calculate_macd(df):
    """Calcula MACD (Moving Average Convergence Divergence)"""
    # EMA 12 e 26 períodos (aproximado por média móvel para simplicidade)
    window_12 = Window.orderBy("Date").rowsBetween(-11, 0)
    window_26 = Window.orderBy("Date").rowsBetween(-25, 0)
    window_9 = Window.orderBy("Date").rowsBetween(-8, 0)
    
    df = df.withColumn("EMA_12", avg("Close").over(window_12))
    df = df.withColumn("EMA_26", avg("Close").over(window_26))
    
    # MACD Line
    df = df.withColumn("MACD", col("EMA_12") - col("EMA_26"))
    
    # Signal Line (média móvel de 9 períodos do MACD)
    df = df.withColumn("MACD_Signal", avg("MACD").over(window_9))
    
    # MACD Histogram
    df = df.withColumn("MACD_Histogram", col("MACD") - col("MACD_Signal"))
    
    # Limpar colunas intermediárias
    df = df.drop("EMA_12", "EMA_26")
    
    return df

def calculate_bollinger_bands(df, period=20, std_dev=2):
    """Calcula Bollinger Bands"""
    window = Window.orderBy("Date").rowsBetween(-(period-1), 0)
    
    df = df.withColumn("BB_Middle", avg("Close").over(window))
    df = df.withColumn("BB_Std", stddev("Close").over(window))
    
    df = df.withColumn("BB_Upper", col("BB_Middle") + (col("BB_Std") * std_dev))
    df = df.withColumn("BB_Lower", col("BB_Middle") - (col("BB_Std") * std_dev))
    df = df.withColumn("BB_Width", col("BB_Upper") - col("BB_Lower"))
    
    # Limpar coluna intermediária
    df = df.drop("BB_Std")
    
    return df

def calculate_atr(df, period=14):
    """Calcula Average True Range (ATR)"""
    # Calcular True Range
    df = df.withColumn("prev_close", lag("Close", 1).over(Window.orderBy("Date")))
    
    df = df.withColumn("tr1", col("High") - col("Low"))
    df = df.withColumn("tr2", spark_abs(col("High") - col("prev_close")))
    df = df.withColumn("tr3", spark_abs(col("Low") - col("prev_close")))
    
    # True Range é o máximo dos três valores
    df = df.withColumn("true_range", 
                       when(col("tr1") >= col("tr2"), 
                            when(col("tr1") >= col("tr3"), col("tr1")).otherwise(col("tr3")))
                       .otherwise(when(col("tr2") >= col("tr3"), col("tr2")).otherwise(col("tr3"))))
    
    # ATR é a média móvel do True Range
    window_atr = Window.orderBy("Date").rowsBetween(-(period-1), 0)
    df = df.withColumn("ATR", avg("true_range").over(window_atr))
    
    # Limpar colunas intermediárias
    df = df.drop("prev_close", "tr1", "tr2", "tr3", "true_range")
    
    return df

def calculate_keltner_channels(df, period=20, multiplier=2):
    """Calcula Keltner Channels"""
    window = Window.orderBy("Date").rowsBetween(-(period-1), 0)
    
    # Calcular ATR primeiro (se ainda não foi calculado)
    if "ATR" not in df.columns:
        df = calculate_atr(df, period)
    
    # Linha central é a EMA do preço típico (ou média móvel simples)
    df = df.withColumn("typical_price", (col("High") + col("Low") + col("Close")) / 3)
    df = df.withColumn("KC_Middle", avg("typical_price").over(window))
    
    # Bandas superior e inferior
    df = df.withColumn("KC_Upper", col("KC_Middle") + (col("ATR") * multiplier))
    df = df.withColumn("KC_Lower", col("KC_Middle") - (col("ATR") * multiplier))
    
    # Limpar coluna intermediária
    df = df.drop("typical_price")
    
    return df

def calculate_stochastic_oscillator(df, period=14, smooth_k=3, smooth_d=3):
    """Calcula Stochastic Oscillator"""
    window = Window.orderBy("Date").rowsBetween(-(period-1), 0)
    
    # Calcular o menor mínimo e maior máximo no período
    df = df.withColumn("lowest_low", spark_min("Low").over(window))
    df = df.withColumn("highest_high", spark_max("High").over(window))
    
    # %K = 100 * (Close - Lowest Low) / (Highest High - Lowest Low)
    df = df.withColumn("stoch_k_raw", 
                       100 * ((col("Close") - col("lowest_low")) / 
                              when((col("highest_high") - col("lowest_low")) == 0, 0.0001)
                              .otherwise(col("highest_high") - col("lowest_low"))))
    
    # Suavizar %K
    window_smooth_k = Window.orderBy("Date").rowsBetween(-(smooth_k-1), 0)
    df = df.withColumn("Stoch_K", avg("stoch_k_raw").over(window_smooth_k))
    
    # %D é a média móvel de %K
    window_smooth_d = Window.orderBy("Date").rowsBetween(-(smooth_d-1), 0)
    df = df.withColumn("Stoch_D", avg("Stoch_K").over(window_smooth_d))
    
    # Limpar colunas intermediárias
    df = df.drop("lowest_low", "highest_high", "stoch_k_raw")
    
    return df

def main():
    if len(sys.argv) != 4:
        print("Uso: transform.py <caminho_input_gcs> <modelo_que_nao_sera_usado> <caminho_output_gcs>")
        sys.exit(-1)
        
    input_path = sys.argv[1]
    output_path = sys.argv[3]
    
    # Iniciar a sessão Spark
    spark = SparkSession.builder.appName("StockTechnicalIndicators").getOrCreate()
    
    print(f"Lendo dados de: {input_path}")
    
    # Ler o arquivo CSV
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    
    # Garantir que a coluna Date está ordenada
    df = df.orderBy("Date")
    
    print("Calculando indicadores técnicos...")
    
    # Calcular todos os indicadores
    df = calculate_moving_averages(df)
    df = calculate_rsi(df)
    df = calculate_macd(df)
    df = calculate_bollinger_bands(df)
    df = calculate_atr(df)
    df = calculate_keltner_channels(df)
    df = calculate_stochastic_oscillator(df)
    
    # Adicionar metadados
    df = df.withColumn("DataTransformacao", current_timestamp())
    df = df.withColumn("Fonte", lit("TechnicalIndicators"))
    
    # Contar linhas
    print(f"Linhas processadas: {df.count()}")
    
    # Salvar resultado
    df.coalesce(1).write.mode("overwrite").csv(output_path, header=True)
    
    print(f"Dados transformados salvos em: {output_path}")
    
    spark.stop()

if __name__ == "__main__":
    main()
