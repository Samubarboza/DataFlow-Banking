import time
import sqlite3
from pathlib import Path

from etl.extract import run_extract_completo
from etl.transform import run_transform
from etl.load import run_load
from etl.utils import log


def run_pipeline():
    inicio = time.time()
    print("\n==============================")
    print("    Ejecutando Pipeline")
    print("==============================\n")

    log("Inicio del pipeline completo")

    try:
        # 1) EXTRACT
        print("→ Extrayendo datos...")
        data = run_extract_completo()
        log("Extracción completa")

        # 2) TRANSFORM
        print("→ Transformando datos...")
        clientes_clean, cuentas_clean, trans_clean = run_transform(data)
        log("Transformación completa")

        # 3) LOAD
        print("→ Cargando datos y generando outputs...")
        run_load(clientes_clean, cuentas_clean, trans_clean)
        log("Carga finalizada")

        print("\n--- Pipeline ejecutado correctamente ---\n")

    except Exception as e:
        log(f"ERROR en pipeline: {e}")

        print("\nError crítico durante la ejecución del pipeline.")
        print(str(e))
        return
    # Ejecución de consultas SQL de muestr
    print("→ Ejecutando consultas demo en SQLite...\n")
    BASE = Path(__file__).resolve().parent
    con = sqlite3.connect(BASE / "db" / "banco.db")

    def query(sql):
        return con.execute(sql).fetchall()

    print("Total de clientes:", query("SELECT COUNT(*) FROM clientes;"))
    print("Saldo promedio por tipo:", query(
        "SELECT tipo_cuenta, AVG(saldo_inicial) FROM cuentas GROUP BY tipo_cuenta;"
    ))
    print("Top movimientos:", query(
        """
        SELECT cli.nombre, SUM(tr.monto) AS total
        FROM transacciones tr
        JOIN cuentas cu ON tr.id_cuenta = cu.id_cuenta
        JOIN clientes cli ON cu.id_cliente = cli.id_cliente
        GROUP BY cli.nombre
        ORDER BY total DESC
        LIMIT 5;
        """
    ))

    con.close()
    # Tiempo tota
    fin = time.time()
    duracion = round(fin - inicio, 2)

    log(f"Pipeline finalizado en {duracion} segundos")
    print(f"\nPipeline completado en {duracion} segundos.\n")


if __name__ == "__main__":
    run_pipeline()
