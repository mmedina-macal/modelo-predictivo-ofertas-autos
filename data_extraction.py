"""
Módulo para extracción de datos desde SQL Server.
Genera queries parametrizadas para entrenamiento y predicción.
"""

import pandas as pd
import numpy as np
from utils import ejecutar_consulta_sql


def _generar_columnas_pivot_kpi(nombre_col_kpi, prefijo, dias):
    """
    Genera el bloque SQL de columnas pivot para un KPI acumulado.
    
    Args:
        nombre_col_kpi: Nombre de la columna del KPI (ej: 'ACUM_MI')
        prefijo: Prefijo para las columnas generadas (ej: 'MI')
        dias: Range de días
        
    Returns:
        String con columnas SQL separadas por comas
    """
    return ",\n        ".join(
        [f"MAX(CASE WHEN DÍAS_REMATE = {d} THEN {nombre_col_kpi} END) AS {prefijo}{abs(d)}" 
         for d in dias]
    )


def _generar_select_columnas(alias_cte, prefijo_col, dias):
    """
    Genera la lista de columnas para el SELECT final.
    
    Args:
        alias_cte: Alias del CTE (ej: 'mi')
        prefijo_col: Prefijo de las columnas (ej: 'MI')
        dias: Range de días
        
    Returns:
        String con columnas SQL separadas por comas
    """
    return ",\n    ".join([f"{alias_cte}.{prefijo_col}{abs(d)}" for d in dias])


def generar_query_entrenamiento(dia_remate, remid):
    """
    Genera la query SQL para obtener datos de entrenamiento.
    
    Args:
        dia_remate: Día relativo al remate (ej: -2, -3, -4)
        remid: ID del remate a excluir de entrenamiento
        
    Returns:
        String con la query SQL completa
    """
    # Ventana de días: desde -14 hasta el día anterior al dia_remate
    dias = range(-15, dia_remate)
    
    # Generar columnas SQL para cada KPI
    cols_ofertas_sql = _generar_columnas_pivot_kpi("ACUM_OFERTAS", "OFERTAS", dias)
    cols_cav_sql = _generar_columnas_pivot_kpi("ACUM_DESCARGASCAV", "CAV", dias)
    cols_insp_sql = _generar_columnas_pivot_kpi("ACUM_DESCARGASINSPTOTALES", "INSP", dias)
    cols_mi_sql = _generar_columnas_pivot_kpi("ACUM_MI", "MI", dias)
    cols_vtot_sql = _generar_columnas_pivot_kpi("ACUM_VISITAS_TOTALES", "VT", dias)
    cols_vu_sql = _generar_columnas_pivot_kpi("ACUM_VISITAS_UNICAS", "VU", dias)
    
    # Generar columnas para SELECT final
    cols_ofertas_select = _generar_select_columnas("o", "OFERTAS", dias)
    cols_cav_select = _generar_select_columnas("cav", "CAV", dias)
    cols_insp_select = _generar_select_columnas("insp", "INSP", dias)
    cols_mi_select = _generar_select_columnas("mi", "MI", dias)
    cols_vtot_select = _generar_select_columnas("vt", "VT", dias)
    cols_vu_select = _generar_select_columnas("vu", "VU", dias)
    
    query = f"""
WITH BaseVehiculos AS (
    SELECT 
        a.PATENTE,
        a.REMID,
        a.LOTEID,
        UPPER(a.FAMILIADESCRIPCION) AS TIPO,
        a.NOMBREMARCA AS MARCA,
        a.NOMBREMODELO AS MODELO,
        b.TIPOCOMBUSTIBLEDESCRIPCION AS COMB,
        a.NOMBREANIO AS AÑO,
        a.KILOMETRAJE,
        a.MINIMO,
        ISNULL(a.MONTOMULTAS, 0) AS MONTOMULTAS,

        CASE
            WHEN UPPER(a.COLOR) LIKE 'BLANCO%' THEN 'BLANCO'
            WHEN UPPER(a.COLOR) LIKE 'NEGRO%' THEN 'NEGRO'
            WHEN UPPER(a.COLOR) LIKE 'GRIS%' THEN 'GRIS'
            WHEN UPPER(a.COLOR) LIKE 'PLATA%' OR UPPER(a.COLOR) LIKE 'PLATEADO%' THEN 'PLATEADO'
            WHEN UPPER(a.COLOR) LIKE 'ROJO%' THEN 'ROJO'
            WHEN UPPER(a.COLOR) LIKE 'AZUL%' THEN 'AZUL'
            WHEN UPPER(a.COLOR) LIKE 'VERDE%' THEN 'VERDE'
            WHEN UPPER(a.COLOR) LIKE 'AMARILLO%' THEN 'AMARILLO'
            WHEN UPPER(a.COLOR) LIKE 'BEIGE%' THEN 'BEIGE'
            WHEN UPPER(a.COLOR) LIKE 'CAFE%' THEN 'CAFE'
            ELSE 'OTRO'
        END AS COLOR,

        c.CILINDESCRIPCION,
        d.TRACCIONDESCRIPCION,
        a.UNIDADNEGOCIODESCRIPCION,
        e.TRANSMISIONDESCRIPCION,
        a.VPCA,
        ISNULL(a.OFERTAS, 0) AS OFERTAS,

        COUNT(*) OVER (PARTITION BY a.REMID, a.NOMBREMARCA, a.NOMBREMODELO) AS CANT_SIMILARES_EN_REMATE,
        COUNT(*) OVER (PARTITION BY a.REMID) AS CANT_LOTES_EN_REMATE,
        YEAR(f.RemFecha) - TRY_CAST(a.NOMBREANIO AS INT) AS ANTIGÜEDAD_VEHICULO

    FROM [Datastage].[dbo].[ModeloOperacionalv2_T_AUTOCAV] a
    LEFT JOIN [Datastage].[dbo].[ModeloOperacionalv2_T_COMBUSTIBLE] b ON a.TIPOCOMBUSTIBLEID = b.TIPOCOMBUSTIBLEID
    LEFT JOIN [Datastage].[dbo].[ModeloOperacionalv2_T_CILINDRADA] c ON a.CILINID = c.CILINID
    LEFT JOIN [Datastage].[dbo].[ModeloOperacionalv2_T_TRACCION] d ON a.TRACCIONID = d.TRACCIONID
    LEFT JOIN [Datastage].[dbo].[ModeloOperacionalv2_T_TRASMISION] e ON a.TRANSMISIONID = e.TRANSMISIONID
    RIGHT JOIN [Datastage].[dbo].[Remates_T_Remate] f ON a.REMID = f.RemId
    WHERE 
        UPPER(f.RemNombre) NOT LIKE '%FICTICIO%'
        AND UPPER(f.RemNombre) NOT LIKE '%PRUEBA%'
        AND f.TipRemId = 3
        AND f.RemFecha <= GETDATE()
        AND f.RemFecha >= '2022-01-01'
        AND a.REMID NOT IN (1370, 1366, 1373)
        AND a.REMID < {remid}
        AND a.REMID >= 1219
        AND a.KILOMETRAJE IS NOT NULL
        AND a.ESTADOBIENDESCRIPCION = 'CONFIRMADO'
),

HHI_por_remate AS (
    SELECT 
        a.REMID,
        a.NOMBREMARCA,
        COUNT(*) * 1.0 / SUM(COUNT(*)) OVER (PARTITION BY a.REMID) AS SHARE_MARCA
    FROM [Datastage].[dbo].[ModeloOperacionalv2_T_AUTOCAV] a
    RIGHT JOIN [Datastage].[dbo].[Remates_T_Remate] f ON a.REMID = f.RemId
    WHERE 
        UPPER(f.RemNombre) NOT LIKE '%FICTICIO%'
        AND UPPER(f.RemNombre) NOT LIKE '%PRUEBA%'
        AND f.TipRemId = 3
        AND f.RemFecha <= GETDATE()
        AND f.RemFecha >= '2022-01-01'
        AND a.REMID NOT IN (1370, 1366, 1373)
        AND a.REMID < {remid}
        AND a.REMID >= 1219
        AND a.KILOMETRAJE IS NOT NULL
        AND a.ESTADOBIENDESCRIPCION = 'CONFIRMADO'
    GROUP BY a.REMID, a.NOMBREMARCA
),

HHI_final AS (
    SELECT 
        REMID,
        SUM(SHARE_MARCA * SHARE_MARCA) AS HHI_MARCA_REMATE
    FROM HHI_por_remate
    GROUP BY REMID
),

Tipo_VPCA_Agrupado AS (
    SELECT 
        v.REMID,
        v.PATENTE,
        COUNT(*) AS CONCENTRACION_TIPO_VPCA
    FROM BaseVehiculos v
    JOIN BaseVehiculos otros
        ON v.REMID = otros.REMID
        AND v.TIPO = otros.TIPO
        AND otros.VPCA BETWEEN v.VPCA * 0.9 AND v.VPCA * 1.1
    GROUP BY v.REMID, v.PATENTE
),

KPI_dia_menos AS (
    SELECT 
        PATENTE,
        REMID,
        ACUM_DESCARGASCAV,
        ACUM_DESCARGASINSPTOTALES,
        ACUM_MI,
        ACUM_VISITAS_TOTALES,
        ACUM_VISITAS_UNICAS,
        ACUM_OFERTAS
    FROM gestion_marketing.dbo.vw_curva_desagregada_kpi_autos
    WHERE DÍAS_REMATE = {dia_remate}
),

OfertasPorDia AS (
    SELECT
        REMID,
        PATENTE,
        {cols_ofertas_sql}
    FROM gestion_marketing.dbo.vw_curva_desagregada_kpi_autos
    WHERE DÍAS_REMATE BETWEEN -15 AND {dia_remate - 1}
    GROUP BY REMID, PATENTE
),

CAVPorDia AS (
    SELECT
        REMID,
        PATENTE,
        {cols_cav_sql}
    FROM gestion_marketing.dbo.vw_curva_desagregada_kpi_autos
    WHERE DÍAS_REMATE BETWEEN -15 AND {dia_remate - 1}
    GROUP BY REMID, PATENTE
),

InspPorDia AS (
    SELECT
        REMID,
        PATENTE,
        {cols_insp_sql}
    FROM gestion_marketing.dbo.vw_curva_desagregada_kpi_autos
    WHERE DÍAS_REMATE BETWEEN -15 AND {dia_remate - 1}
    GROUP BY REMID, PATENTE
),

MiPorDia AS (
    SELECT
        REMID,
        PATENTE,
        {cols_mi_sql}
    FROM gestion_marketing.dbo.vw_curva_desagregada_kpi_autos
    WHERE DÍAS_REMATE BETWEEN -15 AND {dia_remate - 1}
    GROUP BY REMID, PATENTE
),

VTotPorDia AS (
    SELECT
        REMID,
        PATENTE,
        {cols_vtot_sql}
    FROM gestion_marketing.dbo.vw_curva_desagregada_kpi_autos
    WHERE DÍAS_REMATE BETWEEN -15 AND {dia_remate - 1}
    GROUP BY REMID, PATENTE
),

VUPorDia AS (
    SELECT
        REMID,
        PATENTE,
        {cols_vu_sql}
    FROM gestion_marketing.dbo.vw_curva_desagregada_kpi_autos
    WHERE DÍAS_REMATE BETWEEN -15 AND {dia_remate - 1}
    GROUP BY REMID, PATENTE
),

ProyBI AS (
    SELECT 
        REMID, 
        PATENTE, 
        MAX(PROYECCION_BI) AS PROYECCION_BI
    FROM Gestion_data_science.dbo.proyecciones_subastas_autos_bi
    GROUP BY REMID, PATENTE
)

SELECT 
    v.*,
    h.HHI_MARCA_REMATE,
    tv.CONCENTRACION_TIPO_VPCA,
    k.ACUM_DESCARGASCAV,
    k.ACUM_DESCARGASINSPTOTALES,
    k.ACUM_MI,
    k.ACUM_VISITAS_TOTALES,
    k.ACUM_VISITAS_UNICAS,
    k.ACUM_OFERTAS,
    COALESCE(pb.PROYECCION_BI, 0) AS PROYECCION_BI,
    {cols_ofertas_select},
    {cols_cav_select},
    {cols_insp_select},
    {cols_mi_select},
    {cols_vtot_select},
    {cols_vu_select}
FROM BaseVehiculos v
LEFT JOIN HHI_final h      ON v.REMID = h.REMID
LEFT JOIN Tipo_VPCA_Agrupado tv ON v.REMID = tv.REMID AND v.PATENTE = tv.PATENTE
LEFT JOIN KPI_dia_menos k  ON v.PATENTE = k.PATENTE AND v.REMID = k.REMID
LEFT JOIN OfertasPorDia o  ON v.REMID = o.REMID AND v.PATENTE = o.PATENTE
LEFT JOIN CAVPorDia cav    ON v.REMID = cav.REMID AND v.PATENTE = cav.PATENTE
LEFT JOIN InspPorDia insp  ON v.REMID = insp.REMID AND v.PATENTE = insp.PATENTE
LEFT JOIN MiPorDia mi      ON v.REMID = mi.REMID AND v.PATENTE = mi.PATENTE
LEFT JOIN VTotPorDia vt    ON v.REMID = vt.REMID AND v.PATENTE = vt.PATENTE
LEFT JOIN VUPorDia vu      ON v.REMID = vu.REMID AND v.PATENTE = vu.PATENTE
LEFT JOIN ProyBI pb        ON v.REMID = pb.REMID AND v.PATENTE = pb.PATENTE
ORDER BY v.REMID ASC;
"""
    return query


def generar_query_prediccion(dia_remate, remid):
    """
    Genera la query SQL para obtener datos de predicción de un remate específico.
    
    Args:
        dia_remate: Día relativo al remate (ej: -2, -3, -4)
        remid: ID del remate para predicción
        
    Returns:
        String con la query SQL completa
    """
    # Ventana de días: desde -14 hasta el día anterior al dia_remate
    dias = range(-15, dia_remate)
    
    # Generar columnas SQL para cada KPI
    cols_ofertas_sql = _generar_columnas_pivot_kpi("ACUM_OFERTAS", "OFERTAS", dias)
    cols_cav_sql = _generar_columnas_pivot_kpi("ACUM_DESCARGASCAV", "CAV", dias)
    cols_insp_sql = _generar_columnas_pivot_kpi("ACUM_DESCARGASINSPTOTALES", "INSP", dias)
    cols_mi_sql = _generar_columnas_pivot_kpi("ACUM_MI", "MI", dias)
    cols_vtot_sql = _generar_columnas_pivot_kpi("ACUM_VISITAS_TOTALES", "VT", dias)
    cols_vu_sql = _generar_columnas_pivot_kpi("ACUM_VISITAS_UNICAS", "VU", dias)
    
    # Generar columnas para SELECT final
    cols_ofertas_select = _generar_select_columnas("o", "OFERTAS", dias)
    cols_cav_select = _generar_select_columnas("cav", "CAV", dias)
    cols_insp_select = _generar_select_columnas("insp", "INSP", dias)
    cols_mi_select = _generar_select_columnas("mi", "MI", dias)
    cols_vtot_select = _generar_select_columnas("vt", "VT", dias)
    cols_vu_select = _generar_select_columnas("vu", "VU", dias)
    
    query = f"""
WITH BasePred AS (
    SELECT 
        a.REMID,
        a.PATENTE,
        a.LOTEID,
        UPPER(a.FAMILIADESCRIPCION) AS TIPO,
        a.NOMBREMARCA AS MARCA,
        a.NOMBREMODELO AS MODELO,
        b.TIPOCOMBUSTIBLEDESCRIPCION AS COMB,
        a.NOMBREANIO AS AÑO,
        a.KILOMETRAJE,
        a.MINIMO,
        ISNULL(a.MONTOMULTAS, 0) AS MONTOMULTAS,

        CASE
            WHEN UPPER(a.COLOR) LIKE 'BLANCO%' THEN 'BLANCO'
            WHEN UPPER(a.COLOR) LIKE 'NEGRO%' THEN 'NEGRO'
            WHEN UPPER(a.COLOR) LIKE 'GRIS%' THEN 'GRIS'
            WHEN UPPER(a.COLOR) LIKE 'PLATA%' OR UPPER(a.COLOR) LIKE 'PLATEADO%' THEN 'PLATEADO'
            WHEN UPPER(a.COLOR) LIKE 'ROJO%' THEN 'ROJO'
            WHEN UPPER(a.COLOR) LIKE 'AZUL%' THEN 'AZUL'
            WHEN UPPER(a.COLOR) LIKE 'VERDE%' THEN 'VERDE'
            WHEN UPPER(a.COLOR) LIKE 'AMARILLO%' THEN 'AMARILLO'
            WHEN UPPER(a.COLOR) LIKE 'BEIGE%' THEN 'BEIGE'
            WHEN UPPER(a.COLOR) LIKE 'CAFE%' THEN 'CAFE'
            ELSE 'OTRO'
        END AS COLOR,

        c.CILINDESCRIPCION,
        d.TRACCIONDESCRIPCION,
        a.UNIDADNEGOCIODESCRIPCION,
        e.TRANSMISIONDESCRIPCION,
        a.VPCA,

        COUNT(*) OVER (PARTITION BY a.REMID, a.NOMBREMARCA, a.NOMBREMODELO) AS CANT_SIMILARES_EN_REMATE,
        COUNT(*) OVER (PARTITION BY a.REMID) AS CANT_LOTES_EN_REMATE,
        YEAR(f.RemFecha) - TRY_CAST(a.NOMBREANIO AS INT) AS ANTIGÜEDAD_VEHICULO

    FROM [Datastage].[dbo].[ModeloOperacionalv2_T_AUTOCAV] a
    LEFT JOIN [Datastage].[dbo].[ModeloOperacionalv2_T_COMBUSTIBLE] b ON a.TIPOCOMBUSTIBLEID = b.TIPOCOMBUSTIBLEID
    LEFT JOIN [Datastage].[dbo].[ModeloOperacionalv2_T_CILINDRADA] c ON a.CILINID = c.CILINID
    LEFT JOIN [Datastage].[dbo].[ModeloOperacionalv2_T_TRACCION] d ON a.TRACCIONID = d.TRACCIONID
    LEFT JOIN [Datastage].[dbo].[ModeloOperacionalv2_T_TRASMISION] e ON a.TRANSMISIONID = e.TRANSMISIONID
    LEFT JOIN [Datastage].[dbo].[Remates_T_Remate] f ON a.REMID = f.RemId
    WHERE 
        a.REMID = {remid}
        AND a.KILOMETRAJE IS NOT NULL
        AND a.ESTADOBIENDESCRIPCION = 'CONFIRMADO'
),

HHI_pred AS (
    SELECT 
        a.REMID,
        a.NOMBREMARCA,
        COUNT(*) * 1.0 / SUM(COUNT(*)) OVER (PARTITION BY a.REMID) AS SHARE_MARCA
    FROM [Datastage].[dbo].[ModeloOperacionalv2_T_AUTOCAV] a
    WHERE 
        a.REMID = {remid}
        AND a.KILOMETRAJE IS NOT NULL
        AND a.ESTADOBIENDESCRIPCION = 'CONFIRMADO'
    GROUP BY a.REMID, a.NOMBREMARCA
),

HHI_final AS (
    SELECT 
        REMID,
        SUM(SHARE_MARCA * SHARE_MARCA) AS HHI_MARCA_REMATE
    FROM HHI_pred
    GROUP BY REMID
),

TipoVPCA_Agrupado AS (
    SELECT 
        a.REMID,
        a.PATENTE,
        COUNT(*) AS CONCENTRACION_TIPO_VPCA
    FROM BasePred a
    JOIN BasePred b
        ON a.REMID = b.REMID
        AND a.TIPO = b.TIPO
        AND b.VPCA BETWEEN a.VPCA * 0.9 AND a.VPCA * 1.1
    GROUP BY a.REMID, a.PATENTE
),

KPI_dia_menos AS (
    SELECT 
        PATENTE,
        REMID,
        ACUM_DESCARGASCAV,
        ACUM_DESCARGASINSPTOTALES,
        ACUM_MI,
        ACUM_VISITAS_TOTALES,
        ACUM_VISITAS_UNICAS,
        ACUM_OFERTAS
    FROM gestion_marketing.dbo.vw_curva_desagregada_kpi_autos
    WHERE DÍAS_REMATE = {dia_remate}
      AND REMID = {remid}
),

OfertasPorDia AS (
    SELECT 
        REMID,
        PATENTE,
        {cols_ofertas_sql}
    FROM gestion_marketing.dbo.vw_curva_desagregada_kpi_autos
    WHERE DÍAS_REMATE BETWEEN -15 AND {dia_remate - 1}
      AND REMID = {remid}
    GROUP BY REMID, PATENTE
),

CAVPorDia AS (
    SELECT 
        REMID,
        PATENTE,
        {cols_cav_sql}
    FROM gestion_marketing.dbo.vw_curva_desagregada_kpi_autos
    WHERE DÍAS_REMATE BETWEEN -15 AND {dia_remate - 1}
      AND REMID = {remid}
    GROUP BY REMID, PATENTE
),

InspPorDia AS (
    SELECT 
        REMID,
        PATENTE,
        {cols_insp_sql}
    FROM gestion_marketing.dbo.vw_curva_desagregada_kpi_autos
    WHERE DÍAS_REMATE BETWEEN -15 AND {dia_remate - 1}
      AND REMID = {remid}
    GROUP BY REMID, PATENTE
),

MiPorDia AS (
    SELECT 
        REMID,
        PATENTE,
        {cols_mi_sql}
    FROM gestion_marketing.dbo.vw_curva_desagregada_kpi_autos
    WHERE DÍAS_REMATE BETWEEN -15 AND {dia_remate - 1}
      AND REMID = {remid}
    GROUP BY REMID, PATENTE
),

VTotPorDia AS (
    SELECT 
        REMID,
        PATENTE,
        {cols_vtot_sql}
    FROM gestion_marketing.dbo.vw_curva_desagregada_kpi_autos
    WHERE DÍAS_REMATE BETWEEN -15 AND {dia_remate - 1}
      AND REMID = {remid}
    GROUP BY REMID, PATENTE
),

VUPorDia AS (
    SELECT 
        REMID,
        PATENTE,
        {cols_vu_sql}
    FROM gestion_marketing.dbo.vw_curva_desagregada_kpi_autos
    WHERE DÍAS_REMATE BETWEEN -15 AND {dia_remate - 1}
      AND REMID = {remid}
    GROUP BY REMID, PATENTE
),

ProyBI AS (
    SELECT 
        REMID,
        PATENTE,
        MAX(PROYECCION_BI) AS PROYECCION_BI
    FROM Gestion_data_science.dbo.proyecciones_subastas_autos_bi
    WHERE REMID = {remid}
    GROUP BY REMID, PATENTE
)

SELECT 
    p.*,
    h.HHI_MARCA_REMATE,
    v.CONCENTRACION_TIPO_VPCA,
    k.ACUM_DESCARGASCAV,
    k.ACUM_DESCARGASINSPTOTALES,
    k.ACUM_MI,
    k.ACUM_VISITAS_TOTALES,
    k.ACUM_VISITAS_UNICAS,
    k.ACUM_OFERTAS,
    COALESCE(pb.PROYECCION_BI, 0) AS PROYECCION_BI,
    {cols_ofertas_select},
    {cols_cav_select},
    {cols_insp_select},
    {cols_mi_select},
    {cols_vtot_select},
    {cols_vu_select}
FROM BasePred p
LEFT JOIN HHI_final h         ON p.REMID = h.REMID
LEFT JOIN TipoVPCA_Agrupado v ON p.REMID = v.REMID AND p.PATENTE = v.PATENTE
LEFT JOIN KPI_dia_menos k     ON p.PATENTE = k.PATENTE AND p.REMID = k.REMID
LEFT JOIN OfertasPorDia o     ON p.REMID = o.REMID AND p.PATENTE = o.PATENTE
LEFT JOIN CAVPorDia cav       ON p.REMID = cav.REMID AND p.PATENTE = cav.PATENTE
LEFT JOIN InspPorDia insp     ON p.REMID = insp.REMID AND p.PATENTE = insp.PATENTE
LEFT JOIN MiPorDia mi         ON p.REMID = mi.REMID AND p.PATENTE = mi.PATENTE
LEFT JOIN VTotPorDia vt       ON p.REMID = vt.REMID AND p.PATENTE = vt.PATENTE
LEFT JOIN VUPorDia vu         ON p.REMID = vu.REMID AND p.PATENTE = vu.PATENTE
LEFT JOIN ProyBI pb           ON p.REMID = pb.REMID AND p.PATENTE = pb.PATENTE
ORDER BY p.REMID;
"""
    return query


def extraer_datos_entrenamiento(dia_remate=-2, remid=9999):
    """
    Extrae datos de entrenamiento desde SQL Server.
    
    Args:
        dia_remate: Día relativo al remate (default: -2)
        remid: ID del remate a excluir (default: 9999)
        
    Returns:
        DataFrame con datos de entrenamiento
    """
    print(f"Extrayendo datos de entrenamiento (dia_remate={dia_remate}, remid<{remid})...")
    query = generar_query_entrenamiento(dia_remate, remid)
    data = ejecutar_consulta_sql(query)
    
    # Limpiar strings vacíos
    data.replace(r"^\s*$", np.nan, regex=True, inplace=True)
    
    print(f"Datos extraídos: {data.shape[0]} filas, {data.shape[1]} columnas")
    return data


def extraer_datos_prediccion(dia_remate=-2, remid=1432):
    """
    Extrae datos de predicción para un remate específico.
    
    Args:
        dia_remate: Día relativo al remate (default: -2)
        remid: ID del remate para predicción
        
    Returns:
        DataFrame con datos para predicción
    """
    print(f"Extrayendo datos de predicción (dia_remate={dia_remate}, remid={remid})...")
    query = generar_query_prediccion(dia_remate, remid)
    data = ejecutar_consulta_sql(query)
    
    # Limpiar strings vacíos
    data.replace(r"^\s*$", np.nan, regex=True, inplace=True)
    
    print(f"Datos extraídos: {data.shape[0]} filas, {data.shape[1]} columnas")
    return data
