"""
Módulo para el entrenamiento del modelo Random Forest.
Incluye preprocesamiento, entrenamiento con búsqueda de hiperparámetros y evaluación.
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split, RandomizedSearchCV
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import matplotlib.pyplot as plt
from utils import guardar_modelo, crear_directorio_si_no_existe


def preprocesar_datos(data, dia_remate):
    """
    Preprocesa los datos de entrenamiento.
    
    Args:
        data: DataFrame con datos crudos
        dia_remate: Día relativo al remate
        
    Returns:
        Tuple (X, y, columnas_features) donde:
            X: Features preprocesadas
            y: Target (OFERTAS)
            columnas_features: Lista de nombres de columnas de X
    """
    print("Preprocesando datos...")
    
    # Variables categóricas
    categorical_vars = [
        "TIPO", "MARCA", "MODELO", "COMB", "COLOR",
        "CILINDESCRIPCION", "TRACCIONDESCRIPCION",
        "UNIDADNEGOCIODESCRIPCION", "TRANSMISIONDESCRIPCION"
    ]
    
    # Asegurar existencia y normalizar strings vacíos
    for col in categorical_vars:
        if col in data.columns:
            data[col] = data[col].astype(str).str.strip().replace('', 'OTRO')
        else:
            data[col] = 'OTRO'
    
    # One-hot encoding
    data_encoded = pd.get_dummies(data, columns=categorical_vars, drop_first=True)
    
    # Definir columnas de series dinámicas
    dias = range(-15, dia_remate)
    
    cols_ofertas = [f"OFERTAS{abs(d)}" for d in dias]
    cols_cav     = [f"CAV{abs(d)}"     for d in dias]
    cols_insp    = [f"INSP{abs(d)}"    for d in dias]
    cols_mi      = [f"MI{abs(d)}"      for d in dias]
    cols_vt      = [f"VT{abs(d)}"      for d in dias]
    cols_vu      = [f"VU{abs(d)}"      for d in dias]
    
    series_cols = cols_ofertas + cols_cav + cols_insp + cols_mi + cols_vt + cols_vu
    
    # KPI acumulados al día de corte
    kpi_actual = [
        "ACUM_DESCARGASCAV",
        "ACUM_DESCARGASINSPTOTALES",
        "ACUM_MI",
        "ACUM_VISITAS_TOTALES",
        "ACUM_VISITAS_UNICAS",
        "ACUM_OFERTAS"
    ]
    
    # Numéricas base
    base_numeric = [
        "AÑO", "KILOMETRAJE", "MINIMO", "MONTOMULTAS", "VPCA",
        "CANT_SIMILARES_EN_REMATE", "HHI_MARCA_REMATE", "CONCENTRACION_TIPO_VPCA",
        "CANT_LOTES_EN_REMATE", "ANTIGÜEDAD_VEHICULO",
        "PROYECCION_BI"
    ]
    
    # Todas las numéricas
    num_vars = base_numeric + kpi_actual + series_cols
    
    # Convertir a numérico
    for col in num_vars:
        if col in data_encoded.columns:
            data_encoded[col] = pd.to_numeric(data_encoded[col], errors="coerce").fillna(0.0)
    
    # Definir X e y
    target_col = "OFERTAS"
    id_cols = ["REMID", "PATENTE", "LOTEID"]
    
    # Verificar target
    if target_col not in data_encoded.columns:
        raise ValueError(f"No se encontró la columna target '{target_col}' en el DataFrame.")
    
    y = pd.to_numeric(data_encoded[target_col], errors="coerce").fillna(0.0)
    
    # Construir X
    candidate_features = base_numeric + kpi_actual + series_cols
    candidate_features = [c for c in candidate_features if c in data_encoded.columns]
    
    to_drop = [c for c in id_cols if c in data_encoded.columns] + [target_col]
    X = data_encoded.drop(columns=[c for c in to_drop if c in data_encoded.columns], errors="ignore")
    
    # Forzar presencia de numéricas conocidas con 0 si faltan
    for c in candidate_features:
        if c not in X.columns:
            X[c] = 0.0
    
    # Reordenar columnas
    ordered_numeric = [c for c in candidate_features if c in X.columns]
    other_cols = [c for c in X.columns if c not in ordered_numeric]
    X = X[ordered_numeric + other_cols]
    
    print(f"Datos preprocesados: X.shape={X.shape}, y.shape={y.shape}")
    print(f"Total de features: {X.shape[1]}")
    
    return X, y, list(X.columns)


def entrenar_modelo(X, y, n_iter=25, test_size=0.2, random_state=42):
    """
    Entrena un modelo Random Forest con búsqueda de hiperparámetros.
    
    Args:
        X: Features
        y: Target
        n_iter: Número de iteraciones para RandomizedSearchCV
        test_size: Proporción de datos para validación
        random_state: Semilla aleatoria
        
    Returns:
        Tuple (modelo, metricas) donde:
            modelo: Mejor modelo entrenado
            metricas: Dict con métricas de evaluación
    """
    print("\n=== ENTRENAMIENTO DEL MODELO ===")
    
    # Split train/validation
    X_train, X_valid, y_train, y_valid = train_test_split(
        X, y, test_size=test_size, random_state=random_state
    )
    print(f"Train: {X_train.shape[0]} muestras, Valid: {X_valid.shape[0]} muestras")
    
    # Configurar Random Forest
    rf = RandomForestRegressor(random_state=random_state)
    
    # Grilla de hiperparámetros
    param_dist = {
        "n_estimators": [200, 300, 400],
        "max_depth": [None, 12, 20, 30, 40],
        "min_samples_split": [2, 5, 10],
        "min_samples_leaf": [1, 2, 4],
        "max_features": ["sqrt", "log2", None],
        "bootstrap": [True, False]
    }
    
    # Búsqueda de hiperparámetros
    print(f"\nEjecutando RandomizedSearchCV (n_iter={n_iter})...")
    random_search = RandomizedSearchCV(
        estimator=rf,
        param_distributions=param_dist,
        n_iter=n_iter,
        cv=5,
        scoring="r2",
        verbose=1,
        random_state=random_state,
        n_jobs=-1
    )
    
    random_search.fit(X_train, y_train)
    best_rf = random_search.best_estimator_
    
    print("\nMejores hiperparámetros encontrados:")
    print(random_search.best_params_)
    
    # Evaluar en validación
    y_pred = best_rf.predict(X_valid)
    
    mse = mean_squared_error(y_valid, y_pred)
    mae = mean_absolute_error(y_valid, y_pred)
    r2 = r2_score(y_valid, y_pred)
    
    print("\n=== EVALUACIÓN EN SET DE VALIDACIÓN ===")
    print(f"MSE: {mse:.4f}")
    print(f"MAE: {mae:.4f}")
    print(f"R² Score: {r2:.4f}")
    
    metricas = {
        'mse': mse,
        'mae': mae,
        'r2': r2,
        'best_params': random_search.best_params_
    }
    
    # Entrenar modelo final con todos los datos
    print("\nEntrenando modelo final con todos los datos...")
    final_model = RandomForestRegressor(random_state=random_state, **random_search.best_params_)
    final_model.fit(X, y)
    
    return final_model, metricas


def evaluar_modelo(model, X, feature_names, top_n=20, guardar_grafico=True):
    """
    Evalúa el modelo y genera gráfico de importancia de variables.
    
    Args:
        model: Modelo entrenado
        X: Features (para obtener nombres de columnas)
        feature_names: Lista de nombres de features
        top_n: Número de features más importantes a mostrar
        guardar_grafico: Si guardar el gráfico en archivo
        
    Returns:
        DataFrame con importancias de features
    """
    print("\n=== IMPORTANCIA DE VARIABLES ===")
    
    importancias = pd.Series(model.feature_importances_, index=feature_names)
    top_features = importancias.sort_values(ascending=False).head(top_n)
    
    print(f"\nTop {top_n} variables más importantes:")
    for i, (feature, importance) in enumerate(top_features.items(), 1):
        print(f"{i:2d}. {feature:40s}: {importance:.4f}")
    
    # Generar gráfico
    if guardar_grafico:
        crear_directorio_si_no_existe("models")
        
        plt.figure(figsize=(10, 8))
        ax = top_features.sort_values(ascending=True).plot(kind='barh')
        ax.set_title(f"Top {top_n} Variables más Importantes (Random Forest)")
        ax.set_xlabel("Importancia")
        plt.tight_layout()
        plt.savefig("models/feature_importance.png", dpi=300, bbox_inches='tight')
        print(f"\nGráfico guardado en: models/feature_importance.png")
        plt.close()
    
    return importancias.sort_values(ascending=False)


def entrenar_pipeline_completo(data, dia_remate, ruta_modelo="models/modelo_rf.joblib"):
    """
    Pipeline completo de entrenamiento: preprocesamiento + entrenamiento + guardado.
    
    Args:
        data: DataFrame con datos crudos
        dia_remate: Día relativo al remate
        ruta_modelo: Ruta donde guardar el modelo
        
    Returns:
        Dict con modelo, métricas e importancias
    """
    # Preprocesar
    X, y, columnas_features = preprocesar_datos(data, dia_remate)
    
    # Entrenar
    modelo, metricas = entrenar_modelo(X, y)
    
    # Evaluar
    importancias = evaluar_modelo(modelo, X, columnas_features)
    
    # Guardar modelo
    guardar_modelo(modelo, columnas_features, ruta_modelo)
    
    return {
        'modelo': modelo,
        'columnas_features': columnas_features,
        'metricas': metricas,
        'importancias': importancias
    }
