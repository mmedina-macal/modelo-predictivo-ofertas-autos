# Modelo de Predicción de Ofertas en Subastas de Garantías

Modelo de predicción de ofertas finales en subastas de vehículos, utilizando Random Forest y datos históricos acumulados hasta un día específico antes del remate.

## 📋 Descripción

Este proyecto predice el número de **ofertas totales esperadas** por vehículo en subastas, basándose en:

- Variables estructurales del vehículo (marca, modelo, año, kilometraje, etc.)
- KPIs acumulados hasta el día T antes del remate (descargas CAV/Insp, visitas, ofertas parciales, etc.)
- Métricas de concentración de mercado (HHI, similares en remate)

## 🏗️ Arquitectura

```
modelo_github/
├── config.example.py          # Plantilla de configuración
├── config.py                   # Credenciales SQL (NO EN GIT)
├── requirements.txt            # Dependencias
├── .gitignore                 # Exclusiones de Git
├── README.md                  # Esta documentación
│
├── utils.py                   # Utilidades compartidas
├── data_extraction.py         # Extracción de datos SQL
├── train.py                   # Entrenamiento del modelo
├── inference.py               # Predicciones con modelo
│
├── main_train.py              # Script de entrenamiento
├── main_inference.py          # Script de predicción
│
└── models/                    # Modelos guardados
    └── modelo_rf.joblib
```

## 🚀 Instalación

### 1. Clonar el repositorio

```bash
git clone <URL_DEL_REPO>
cd modelo_github
```

### 2. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 3. Configurar credenciales SQL

```bash
# Copiar la plantilla
copy config.example.py config.py

# Editar config.py con tus credenciales reales
# IMPORTANTE: config.py NO debe subirse a Git
```

**Contenido de `config.py`:**

```python
SQL_SERVER = "10.10.90.175"
SQL_USER = "tu_usuario"
SQL_PASSWORD = "tu_contraseña"
SQL_DATABASE = "gestion_marketing"
```

## 📖 Uso

### Entrenamiento

Entrena un modelo con datos históricos excluyendo remates recientes:

```bash
# Entrenamiento básico (día -2, excluir REMID < 1432)
python main_train.py --dia-remate -2 --remid 1432

# Entrenamiento desde día -3
python main_train.py --dia-remate -3 --remid 1433

# Guardar modelo en ruta personalizada
python main_train.py --dia-remate -2 --remid 1432 --output-model models/modelo_d3.joblib
```

**Argumentos:**

- `--dia-remate`: Día relativo al remate para features (ej: -2, -3, -4)
- `--remid`: Excluir remates con ID >= este valor
- `--output-model`: Ruta donde guardar el modelo (default: `models/modelo_rf.joblib`)

### Predicción

Predice ofertas para un remate específico:

```bash
# Predicción para remate 1432, fecha 2025-12-11, día -2
python main_inference.py --remid 1432 --fecha-subasta 2025-12-11 --dia-remate -2

# Modo TEST (no sube a SQL, agrega sufijo _test al Excel)
python main_inference.py --remid 1432 --fecha-subasta 2025-12-11 --test

# Solo guardar Excel (no subir a SQL)
python main_inference.py --remid 1432 --fecha-subasta 2025-12-11 --no-sql

# Usar modelo personalizado
python main_inference.py --remid 1432 --fecha-subasta 2025-12-11 --input-model models/modelo_d3.joblib
```

**Argumentos:**

- `--remid` _(requerido)_: ID del remate para predecir
- `--fecha-subasta` _(requerido)_: Fecha de la subasta (YYYY-MM-DD)
- `--dia-remate`: Día relativo al remate (default: -2)
- `--test`: Modo prueba (no sube a SQL)
- `--no-sql`: No subir a SQL (solo Excel)
- `--input-model`: Ruta del modelo entrenado

### Salidas

**Entrenamiento:**

- Modelo entrenado: `models/modelo_rf.joblib`
- Gráfico de importancia: `models/feature_importance.png`
- Métricas en consola (MSE, MAE, R²)

**Predicción:**

- Excel: `Proyecciones/Subasta_{fecha}/Proyecciones_{dia_remate}.xlsx`
- SQL: Tabla `gestion_marketing.dbo.predicciones_ofertas_subastas`

## 🔧 Configuración para GCP

### Cloud Functions (Recomendado)

1. **Estructura para Cloud Function:**

```
function_predict/
├── main.py              # Wrapper de main_inference.py
├── requirements.txt
├── data_extraction.py
├── inference.py
├── utils.py
└── models/
    └── modelo_rf.joblib
```

2. **Credenciales:**

- Usar Secret Manager para `config.py`
- Variables de entorno para parámetros dinámicos

### Cloud Run

Similar a Cloud Functions, pero en contenedor Docker.

**Dockerfile:**

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["python", "main_inference.py"]
```

## 📊 Datos

### Fuentes

- **Datastage**: Datos estructurales de vehículos
- **gestion_marketing.vw_curva_desagregada_kpi_autos**: KPIs diarios acumulados
- **Gestion_data_science.proyecciones_subastas_autos_bi**: Proyecciones BI

### Features Principales

1. **Estructurales:** TIPO, MARCA, MODELO, AÑO, KILOMETRAJE, VPCA, etc.
2. **KPIs Acumulados:** ACUM_OFERTAS, ACUM_VISITAS_TOTALES, ACUM_MI, etc.
3. **Series Temporales:** Ofertas/Visitas/Descargas por día (-15 a día_remate-1)
4. **Concentración:** HHI_MARCA_REMATE, CONCENTRACION_TIPO_VPCA

### Target

- **OFERTAS**: Número final de ofertas al cierre del remate (día 0)

## 🧪 Validación

```bash
# Entrenar modelo con datos hasta remate 1431
python main_train.py --dia-remate -2 --remid 1432

# Predecir remate 1432
python main_inference.py --remid 1432 --fecha-subasta 2025-12-11 --test

# Comparar predicciones con valores reales (cuando estén disponibles)
```

## 🤝 Contribuciones

Este proyecto es interno de Macal. Para cambios:

1. Crear rama feature
2. Implementar cambios
3. Pull request con revisión

## 📝 Notas Importantes

- **NUNCA** subir `config.py` a Git
- Los modelos guardados (`.joblib`) son grandes, no subirlos a Git
- Parámetros configurables: día del remate, REMID, fecha de subasta
- El modelo aprende patrones entre información parcial (día T) y resultado final

## 📞 Contacto

Para dudas: Data Science Team - Macal

---

**Última actualización:** 2025-12-10
