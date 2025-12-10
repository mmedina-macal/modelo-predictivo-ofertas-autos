"""
Script principal para entrenar el modelo de predicción de garantías.

Uso:
    python main_train.py --dia-remate -2 --remid 1432
    python main_train.py --dia-remate -3 --remid 1433
"""

import argparse
from data_extraction import extraer_datos_entrenamiento
from train import entrenar_pipeline_completo


def parse_args():
    """Parsea argumentos de línea de comandos."""
    parser = argparse.ArgumentParser(
        description="Entrenar modelo de predicción de ofertas en subastas"
    )
    
    parser.add_argument(
        "--dia-remate",
        type=int,
        default=-2,
        help="Día relativo al remate para entrenamiento (ej: -2, -3, -4). Default: -2"
    )
    
    parser.add_argument(
        "--remid",
        type=int,
        default=9999,
        help="ID del remate a EXCLUIR del entrenamiento. Default: 9999"
    )
    
    parser.add_argument(
        "--output-model",
        type=str,
        default="models/modelo_rf.joblib",
        help="Ruta donde guardar el modelo entrenado. Default: models/modelo_rf.joblib"
    )
    
    return parser.parse_args()


def main():
    """Función principal de entrenamiento."""
    # Parsear argumentos
    args = parse_args()
    
    print("=" * 80)
    print("ENTRENAMIENTO DE MODELO DE PREDICCIÓN DE OFERTAS")
    print("=" * 80)
    print(f"\nParámetros:")
    print(f"  Día del remate: {args.dia_remate}")
    print(f"  REMID a excluir: < {args.remid}")
    print(f"  Modelo de salida: {args.output_model}")
    print()
    
    # Extraer datos de entrenamiento
    print("\n" + "=" * 80)
    print("PASO 1: EXTRACCIÓN DE DATOS")
    print("=" * 80)
    data = extraer_datos_entrenamiento(
        dia_remate=args.dia_remate,
        remid=args.remid
    )
    
    # Entrenar modelo
    print("\n" + "=" * 80)
    print("PASO 2: ENTRENAMIENTO DEL MODELO")
    print("=" * 80)
    resultado = entrenar_pipeline_completo(
        data=data,
        dia_remate=args.dia_remate,
        ruta_modelo=args.output_model
    )
    
    # Resumen final
    print("\n" + "=" * 80)
    print("ENTRENAMIENTO COMPLETADO")
    print("=" * 80)
    print(f"\nMétricas del modelo:")
    print(f"  MSE: {resultado['metricas']['mse']:.4f}")
    print(f"  MAE: {resultado['metricas']['mae']:.4f}")
    print(f"  R²:  {resultado['metricas']['r2']:.4f}")
    print(f"\nModelo guardado en: {args.output_model}")
    print(f"Total de features: {len(resultado['columnas_features'])}")
    print()


if __name__ == "__main__":
    main()
