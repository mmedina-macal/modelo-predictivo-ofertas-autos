"""
Módulo para realizar predicciones con el modelo entrenado.
Incluye preprocesamiento alineado con entrenamiento y guardado de resultados.
"""

import pandas as pd
import numpy as np
import os
from utils import cargar_modelo, crear_directorio_si_no_existe, subir_dataframe_sql


def preprocesar_datos_inferencia(data, columnas_entrenamiento, dia_remate):
    """
    Preprocesa datos para inferencia, alineando con las columnas del entrenamiento.
    
    Args:
        data: DataFrame con datos crudos
        columnas_entrenamiento: Lista de columnas usadas en entrenamiento
        dia_remate: Día relativo al remate
        
    Returns:
        DataFrame preprocesado con las mismas columnas que el entrenamiento
    """
    print("Preprocesando datos para inferencia...")
    
    # Variables categóricas (mismo orden que entrenamiento)
    categorical_vars = [
        "TIPO", "MARCA", "MODELO", "COMB", "COLOR",
        "CILINDESCRIPCION", "TRACCIONDESCRIPCION",
        "UNIDADNEGOCIODESCRIPCION", "TRANSMISIONDESCRIPCION"
    ]
    
    # Asegurar existencia y normalizar
    for col in categorical_vars:
        if col in data.columns:
            data[col] = data[col].astype(str).str.strip().replace('', 'OTRO')
        else:
            data[col] = 'OTRO'
    
    # One-hot encoding (mismo que entrenamiento)
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
    
    # KPI acumulados
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
    
    num_vars = base_numeric + kpi_actual + series_cols
    
    # Convertir a numérico
    for col in num_vars:
        if col in data_encoded.columns:
            data_encoded[col] = pd.to_numeric(data_encoded[col], errors="coerce").fillna(0.0)
    
    # Quitar duplicados de columnas si los hubiese
    data_encoded = data_encoded.loc[:, ~data_encoded.columns.duplicated()]
    
    # CRÍTICO: Alinear columnas con las del entrenamiento
    # Esto asegura que X tenga exactamente las mismas columnas en el mismo orden
    data_encoded = data_encoded.reindex(columns=columnas_entrenamiento, fill_value=0.0)
    
    print(f"Datos preprocesados: shape={data_encoded.shape}")
    print(f"Columnas alineadas con entrenamiento: {len(columnas_entrenamiento)}")
    
    return data_encoded


def predecir(modelo, data_procesada):
    """
    Ejecuta predicciones con el modelo.
    
    Args:
        modelo: Modelo Random Forest entrenado
        data_procesada: DataFrame preprocesado
        
    Returns:
        Array con predicciones
    """
    print("\nRealizando predicciones...")
    predicciones = modelo.predict(data_procesada)
    print(f"Predicciones generadas: {len(predicciones)} valores")
    return predicciones


def guardar_predicciones_excel(df, dia_remate, fecha_subasta, test=False):
    """
    Guarda las predicciones en archivo Excel.
    
    Args:
        df: DataFrame con predicciones
        dia_remate: Día relativo al remate
        fecha_subasta: Fecha de la subasta (YYYY-MM-DD)
        test: Si es modo test (agrega sufijo _test)
        
    Returns:
        Ruta del archivo guardado
    """
    postfix = '_test' if test else ''
    ruta_directorio = f"Proyecciones/Subasta_{fecha_subasta}"
    crear_directorio_si_no_existe(ruta_directorio)
    
    ruta_archivo = f"{ruta_directorio}/Proyecciones_{dia_remate}{postfix}.xlsx"
    df.to_excel(ruta_archivo, index=False)
    
    print(f"\nPredicciones guardadas en: {ruta_archivo}")
    return ruta_archivo


def preparar_datos_sql(df, dia_remate):
    """
    Prepara DataFrame para subir a SQL.
    
    Args:
        df: DataFrame con predicciones
        dia_remate: Día relativo al remate
        
    Returns:
        DataFrame limpio listo para SQL
    """
    df_subir = df.copy()
    
    # Agregar columna DIA_REMATE
    df_subir["DIA_REMATE"] = int(dia_remate)
    
    # Seleccionar columnas requeridas
    cols_final = [
        "REMID", "PATENTE", "LOTEID", "TIPO", "MARCA", "MODELO",
        "AÑO", "COMB", "KILOMETRAJE", "MINIMO", "COLOR",
        "OFERTAS_PREDICHAS_RF", "DIA_REMATE"
    ]
    df_subir = df_subir[[c for c in cols_final if c in df_subir.columns]].copy()
    
    # Tipificar columnas
    text_cols = ["PATENTE", "TIPO", "MARCA", "MODELO", "COMB", "COLOR"]
    for c in text_cols:
        if c in df_subir.columns:
            df_subir[c] = df_subir[c].astype(str)
    
    int_cols = ["REMID", "LOTEID", "AÑO", "KILOMETRAJE", "DIA_REMATE"]
    for c in int_cols:
        if c in df_subir.columns:
            df_subir[c] = pd.to_numeric(df_subir[c], errors="coerce").astype("Int64")
    
    float_cols = ["MINIMO", "OFERTAS_PREDICHAS_RF"]
    for c in float_cols:
        if c in df_subir.columns:
            df_subir[c] = pd.to_numeric(df_subir[c], errors="coerce").astype(float).round(2)
    
    # Eliminar duplicados por PK
    pk = ["REMID", "PATENTE", "LOTEID", "DIA_REMATE"]
    cols_pk_presentes = [c for c in pk if c in df_subir.columns]
    
    if cols_pk_presentes == pk:
        dups_mask = df_subir.duplicated(subset=pk, keep='last')
        n_dups = int(dups_mask.sum())
        if n_dups > 0:
            print(f"Eliminando {n_dups} duplicados por PK {pk}.")
            df_subir = df_subir[~dups_mask].copy()
    
    # Reemplazar inf/NaN por None
    df_subir = df_subir.replace([float('inf'), float('-inf')], pd.NA)
    df_subir = df_subir.where(pd.notnull(df_subir), None)
    
    return df_subir


def subir_predicciones_sql(df, dia_remate, tabla="predicciones_ofertas_subastas", schema="dbo"):
    """
    Sube predicciones a SQL Server.
    
    Args:
        df: DataFrame con predicciones completas
        dia_remate: Día relativo al remate
        tabla: Nombre de la tabla destino
        schema: Schema de la tabla
    """
    print("\n=== SUBIENDO PREDICCIONES A SQL ===")
    
    # Preparar datos
    df_sql = preparar_datos_sql(df, dia_remate)
    
    print(f"Registros a subir: {len(df_sql)}")
    print(f"Tabla destino: {schema}.{tabla}")
    
    # Subir a SQL
    subir_dataframe_sql(df_sql, tabla, schema)


def pipeline_inferencia_completo(data_raw, dia_remate, fecha_subasta, remid, 
                                   test=False, subir_sql=True,
                                   ruta_modelo="models/modelo_rf.joblib"):
    """
    Pipeline completo de inferencia: carga modelo + preprocesamiento + predicción + guardado.
    
    Args:
        data_raw: DataFrame con datos crudos de predicción
        dia_remate: Día relativo al remate
        fecha_subasta: Fecha de la subasta (YYYY-MM-DD)
        remid: ID del remate
        test: Si es modo test
        subir_sql: Si subir resultados a SQL
        ruta_modelo: Ruta del modelo entrenado
        
    Returns:
        DataFrame con predicciones
    """
    # Cargar modelo
    modelo_data = cargar_modelo(ruta_modelo)
    modelo = modelo_data['modelo']
    columnas_features = modelo_data['columnas_features']
    
    # Preprocesar datos
    data_procesada = preprocesar_datos_inferencia(data_raw, columnas_features, dia_remate)
    
    # Predecir
    predicciones = predecir(modelo, data_procesada)
    
    # Agregar predicciones al DataFrame original
    data_raw["OFERTAS_PREDICHAS_RF"] = predicciones
    
    # Guardar en Excel
    ruta_excel = guardar_predicciones_excel(data_raw, dia_remate, fecha_subasta, test)
    
    # Subir a SQL si se solicita
    if subir_sql and not test:
        subir_predicciones_sql(data_raw, dia_remate)
    elif test:
        print("\nModo TEST: No se suben datos a SQL")
    
    print("\n=== PIPELINE DE INFERENCIA COMPLETADO ===")
    return data_raw
