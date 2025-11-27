#gsutil cp stock_rf_model.pkl gs://mydataproc-pdm/models/

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import pickle
import matplotlib.pyplot as plt
import seaborn as sns

def prepare_data(df):
    """
    Prepara os dados para treinamento
    - Remove linhas com NaN (primeiras linhas dos indicadores)
    - Cria a variável target (preço do próximo dia)
    - Seleciona as features
    """
    # Criar target: preço do próximo dia
    df['Target'] = df['Close'].shift(-1)
    
    # Remover última linha (não tem target) e primeiras linhas (NaN nos indicadores)
    df = df.dropna()
    
    # Selecionar features (todos os indicadores técnicos)
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
    
    # Verificar quais colunas realmente existem
    available_features = [col for col in feature_columns if col in df.columns]
    
    X = df[available_features]
    y = df['Target']
    
    return X, y, available_features

def train_model(X, y):
    """
    Treina o modelo Random Forest
    """
    # Split train/test (80/20)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, shuffle=False  # shuffle=False para manter ordem temporal
    )
    
    print(f"Tamanho do treino: {len(X_train)}")
    print(f"Tamanho do teste: {len(X_test)}")
    
    # Criar e treinar modelo
    print("\nTreinando Random Forest...")
    model = RandomForestRegressor(
        n_estimators=500,
        max_depth=20,
        min_samples_split=5,
        min_samples_leaf=2,
        random_state=42,
        n_jobs=-1,
        verbose=1
    )
    
    model.fit(X_train, y_train)
    
    # Fazer predições
    y_pred_train = model.predict(X_train)
    y_pred_test = model.predict(X_test)
    
    # Avaliar modelo
    print("\n" + "="*50)
    print("MÉTRICAS DE PERFORMANCE")
    print("="*50)
    
    print("\nTREINO:")
    print(f"  R² Score: {r2_score(y_train, y_pred_train):.4f}")
    #print(f"  RMSE: ${mean_squared_error(y_train, y_pred_train, squared=False):.2f}")
    print(f"  MAE: ${mean_absolute_error(y_train, y_pred_train):.2f}")
    
    print("\nTESTE:")
    print(f"  R² Score: {r2_score(y_test, y_pred_test):.4f}")
    #print(f"  RMSE: ${mean_squared_error(y_test, y_pred_test, squared=False):.2f}")
    print(f"  MAE: ${mean_absolute_error(y_test, y_pred_test):.2f}")
    
    # Feature importance
    print("\n" + "="*50)
    print("TOP 10 FEATURES MAIS IMPORTANTES")
    print("="*50)
    
    feature_importance = pd.DataFrame({
        'feature': X.columns,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    print(feature_importance.head(10).to_string(index=False))
    
    return model, X_train, X_test, y_train, y_test, y_pred_test

def plot_results(y_test, y_pred_test):
    """
    Plota gráficos de performance
    """
    fig, axes = plt.subplots(2, 1, figsize=(14, 10))
    
    # Gráfico 1: Predição vs Real
    axes[0].plot(range(len(y_test)), y_test.values, label='Real', linewidth=2, alpha=0.7)
    axes[0].plot(range(len(y_pred_test)), y_pred_test, label='Predição', linewidth=2, alpha=0.7)
    axes[0].set_title('Preço Real vs Predição', fontsize=14, fontweight='bold')
    axes[0].set_xlabel('Dias')
    axes[0].set_ylabel('Preço ($)')
    axes[0].legend()
    axes[0].grid(True, alpha=0.3)
    
    # Gráfico 2: Erro de predição
    errors = y_test.values - y_pred_test
    axes[1].plot(range(len(errors)), errors, color='red', linewidth=1, alpha=0.6)
    axes[1].axhline(y=0, color='black', linestyle='--', linewidth=1)
    axes[1].fill_between(range(len(errors)), errors, 0, alpha=0.3, color='red')
    axes[1].set_title('Erro de Predição (Real - Predição)', fontsize=14, fontweight='bold')
    axes[1].set_xlabel('Dias')
    axes[1].set_ylabel('Erro ($)')
    axes[1].grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('model_performance.png', dpi=300, bbox_inches='tight')
    print("\n📊 Gráfico salvo como 'model_performance.png'")
    plt.show()

def save_model(model, filename='stock_rf_model.pkl'):
    """
    Salva o modelo treinado
    """
    with open(filename, 'wb') as f:
        pickle.dump(model, f)
    print(f"\n✅ Modelo salvo como '{filename}'")

def main():
    # Carregar dados processados
    print("Carregando dados...")

    #df = pd.read_csv('stock_data_with_indicators.csv')
    files = [
        "stock_data_with_indicators.csv",
        "CAT.csv",
        "MSTR.csv",
        "PFE.csv"
    ]
    df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)


    # Preparar dados
    print("\nPreparando dados...")
    X, y, feature_names = prepare_data(df)
    
    print(f"\nFeatures utilizadas: {len(feature_names)}")
    print(f"Total de exemplos: {len(X)}")
    
    # Treinar modelo
    model, X_train, X_test, y_train, y_test, y_pred_test = train_model(X, y)
    
    # Plotar resultados
    print("\nGerando gráficos...")
    plot_results(y_test, y_pred_test)
    
    # Salvar modelo
    save_model(model)
    
    # Exemplo de predição
    print("\n" + "="*50)
    print("EXEMPLO DE PREDIÇÃO")
    print("="*50)
    print(f"Último preço real do conjunto de teste: ${y_test.iloc[-1]:.2f}")
    print(f"Predição do modelo: ${y_pred_test[-1]:.2f}")
    print(f"Diferença: ${abs(y_test.iloc[-1] - y_pred_test[-1]):.2f}")
    
    print("\n🎉 Treinamento concluído com sucesso!")

if __name__ == "__main__":
    main()
