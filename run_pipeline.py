import time

from etl.extract import run_extract_completo
from etl.transform import run_transform
from etl.load import run_load
from etl.utils import log
from sql.demo_queries import consulta_sql


def run_pipeline():
    inicio = time.time()
    print("\n==============================\n    Ejecutando Pipeline\n==============================\n")

    log("Inicio del pipeline completo")

    try:
        # 1) EXTRACT
        print("Extrayendo datos...")
        data = run_extract_completo()
        log("Extracción completa")

        # 2) TRANSFORM
        print("Transformando datos...")
        clientes_clean, cuentas_clean, trans_clean = run_transform(data)
        log("Transformación completa")

        # 3) LOAD
        print("Cargando datos y generando outputs...")
        run_load(clientes_clean, cuentas_clean, trans_clean)
        log("Carga finalizada")

        print("\n--- Pipeline ejecutado correctamente ---\n")

    except Exception as e:
        log(f"ERROR en pipeline: {e}")

        print(f"\nError crítico durante la ejecución del pipeline.\n{str(e)}")
        return
    
    # consulta sql
    log("Ejecutando consulta SQL...")
    consulta_sql()
    log("Consulta SQL finalizada")

    # Tiempo tota
    fin = time.time()
    duracion = round(fin - inicio, 2) # redondeamos el resultado con 2 decimales nada mas

    log(f"Pipeline finalizado en {duracion} segundos")
    print(f"\nPipeline completado en {duracion} segundos.\n")


if __name__ == "__main__":
    run_pipeline()
