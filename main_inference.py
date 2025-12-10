"""
Script principal para realizar predicciones con el modelo entrenado.

Uso:
    python main_inference.py --remid 1432 --fecha-subasta 2025-12-11 --dia-remate -2
    python main_inference.py --remid 1432 --fecha-subasta 2025-12-11 --test
"""

import argparse
from data_extraction import extraer_datos_prediccion
from inference import pipeline_inferencia_completo


def parse_args():
    """Parsea argumentos de línea de comandos."""
    parser = argparse.ArgumentParser(
        description="Predecir ofertas para un remate específico"
    )
    
    parser.add_argument(
        "--remid",
        type=int,
        required=True,
        help="ID del remate para realizar predicciones (requerido)"
    )
    
    parser.add_argument(
        "--fecha-subasta",
        type=str,
        required=True,
        help="Fecha de la subasta en formato YYYY-MM-DD (requerido)"
    )
    
    parser.add_argument(
        "--dia-remate",
        type=int,
        default=-2,
        help="Día relativo al remate (ej: -2, -3, -4). Default: -2"
    )
    
    parser.add_argument(
        "--test",
        action="store_true",
        help="Modo prueba: agrega sufijo _test y NO sube a SQL"
    )
    
    parser.add_argument(
        "--input-model",
        type=str,
        default="models/modelo_rf.joblib",
        help="Ruta del modelo entrenado. Default: models/modelo_rf.joblib"
    )
    
    parser.add_argument(
        "--no-sql",
        action="store_true",
        help="No subir predicciones a SQL (solo guardar Excel)"
    )
    
    return parser.parse_args()


def main():
    """Función principal de inferencia."""
    # Parsear argumentos
    args = parse_args()
    
    print("=" * 80)
    print("PREDICCIÓN DE OFERTAS EN SUBASTAS")
    print("=" * 80)
    print(f"\nParámetros:")
    print(f"  REMID: {args.remid}")
    print(f"  Fecha subasta: {args.fecha_subasta}")
    print(f"  Día del remate: {args.dia_remate}")
    print(f"  Modo TEST: {args.test}")
    print(f"  Subir a SQL: {not args.no_sql and not args.test}")
    print(f"  Modelo: {args.input_model}")
    print()
    
    # Extraer datos de predicción
    print("\n" + "=" * 80)
    print("PASO 1: EXTRACCIÓN DE DATOS")
    print("=" * 80)
    data = extraer_datos_prediccion(
        dia_remate=args.dia_remate,
        remid=args.remid
    )
    
    # Realizar predicciones
    print("\n" + "=" * 80)
    print("PASO 2: PREDICCIÓN")
    print("=" * 80)
    resultados = pipeline_inferencia_completo(
        data_raw=data,
        dia_remate=args.dia_remate,
        fecha_subasta=args.fecha_subasta,
        remid=args.remid,
        test=args.test,
        subir_sql=not args.no_sql,
        ruta_modelo=args.input_model
    )
    
    # Resumen final
    print("\n" + "=" * 80)
    print("PREDICCIÓN COMPLETADA")
    print("=" * 80)
    print(f"\nTotal de predicciones: {len(resultados)}")
    print(f"Ofertas predichas (promedio): {resultados['OFERTAS_PREDICHAS_RF'].mean():.2f}")
    print(f"Ofertas predichas (min-max): {resultados['OFERTAS_PREDICHAS_RF'].min():.2f} - {resultados['OFERTAS_PREDICHAS_RF'].max():.2f}")
    print()


if __name__ == "__main__":
    main()
