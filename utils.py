"""
Utilidades comunes para el modelo de predicción de garantías.
"""

import pyodbc
import pandas as pd
import joblib
import os
from pathlib import Path
from config import SQL_SERVER, SQL_USER, SQL_PASSWORD, SQL_DATABASE


def get_sql_connection(database=None):
    """
    Crea y retorna una conexión a SQL Server.
    
    Args:
        database: Nombre de la base de datos. Si es None, usa SQL_DATABASE de config.
        
    Returns:
        Objeto de conexión pyodbc
    """
    db = database or SQL_DATABASE
    conn_str = (
        f"DRIVER={{SQL Server}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={db};"
        f"UID={SQL_USER};"
        f"PWD={SQL_PASSWORD};"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)


def ejecutar_consulta_sql(query, database=None):
    """
    Ejecuta una consulta SQL y retorna los resultados como DataFrame.
    
    Args:
        query: String con la consulta SQL
        database: Nombre de la base de datos (opcional)
        
    Returns:
        DataFrame con los resultados
    """
    conexion = get_sql_connection(database)
    cursor = conexion.cursor()
    cursor.execute(query)
    
    # Obtener resultados y nombres de columnas
    resultados = cursor.fetchall()
    columnas = [column[0] for column in cursor.description]
    
    conexion.close()
    
    # Convertir a DataFrame
    resultados_lista = [list(fila) for fila in resultados]
    df = pd.DataFrame(resultados_lista, columns=columnas)
    
    return df


def crear_directorio_si_no_existe(ruta):
    """
    Crea un directorio si no existe.
    
    Args:
        ruta: Ruta del directorio a crear
    """
    Path(ruta).mkdir(parents=True, exist_ok=True)


def guardar_modelo(modelo, columnas_features, ruta_modelo="models/modelo_rf.joblib"):
    """
    Guarda el modelo entrenado y las columnas de features.
    
    Args:
        modelo: Modelo entrenado de scikit-learn
        columnas_features: Lista de nombres de columnas usadas en entrenamiento
        ruta_modelo: Ruta donde guardar el modelo
    """
    # Crear directorio si no existe
    crear_directorio_si_no_existe(os.path.dirname(ruta_modelo))
    
    # Guardar modelo y metadatos
    modelo_data = {
        'modelo': modelo,
        'columnas_features': columnas_features
    }
    joblib.dump(modelo_data, ruta_modelo)
    print(f"Modelo guardado en: {ruta_modelo}")


def cargar_modelo(ruta_modelo="models/modelo_rf.joblib"):
    """
    Carga un modelo entrenado previamente.
    
    Args:
        ruta_modelo: Ruta del modelo a cargar
        
    Returns:
        Dict con 'modelo' y 'columnas_features'
    """
    if not os.path.exists(ruta_modelo):
        raise FileNotFoundError(f"No se encontró el modelo en: {ruta_modelo}")
    
    modelo_data = joblib.load(ruta_modelo)
    print(f"Modelo cargado desde: {ruta_modelo}")
    return modelo_data


def subir_dataframe_sql(df, tabla_destino, schema="dbo", database=None):
    """
    Sube un DataFrame a SQL Server usando fast_executemany.
    
    Args:
        df: DataFrame a subir
        tabla_destino: Nombre de la tabla destino
        schema: Schema de la tabla (default: 'dbo')
        database: Base de datos (opcional)
    """
    columnas = list(df.columns)
    valores = [tuple(x) for x in df.to_numpy()]
    
    placeholders = ",".join(["?"] * len(columnas))
    cols_sql = ",".join([f"[{c}]" for c in columnas])
    insert_sql = f"INSERT INTO {schema}.{tabla_destino} ({cols_sql}) VALUES ({placeholders})"
    
    with get_sql_connection(database) as conexion:
        cursor = conexion.cursor()
        cursor.fast_executemany = True
        cursor.executemany(insert_sql, valores)
        conexion.commit()
    
    print(f"Datos subidos exitosamente a {schema}.{tabla_destino}")
