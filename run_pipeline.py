import sqlite3
from pathlib import Path

from etl.extract import run_extract
from etl.transform import run_transform
from etl.load import run_load


def ejecutar_consulta(con, sql):
    cur = con.cursor()
    cur.execute(sql)
    return cur.fetchall()


def mostrar_resultado(nombre, filas):
    print(f"\n=== {nombre} ===")
    for fila in filas:
        print(fila)


def run_pipeline():
    print("\n==============================")
    print("  Ejecutando Pipeline Completo")
    print("==============================\n")

    # 1) Extract
    raw_clientes, raw_cuentas, raw_trans = run_extract()

    # 2) Transform
    clientes_clean, cuentas_clean, trans_clean = run_transform(
        raw_clientes, raw_cuentas, raw_trans
    )

    # 3) Load
    run_load(clientes_clean, cuentas_clean, trans_clean)
    print(clientes_clean)

    print("\n--- Pipeline ejecutado ---\n")

    # 4) Ejecutar un par de consultas SQL para mostrar "poder"
    BASE = Path(__file__).resolve().parent
    con = sqlite3.connect(BASE / "db" / "banco.db")

    # Consulta 1: total de clientes
    filas = ejecutar_consulta(con, "SELECT COUNT(*) FROM clientes;")
    mostrar_resultado("Total de clientes", filas)

    # Consulta 2: saldo promedio por tipo de cuenta
    filas = ejecutar_consulta(
        con,
        """
        SELECT tipo_cuenta, AVG(saldo_inicial)
        FROM cuentas
        GROUP BY tipo_cuenta;
        """
    )
    mostrar_resultado("Saldo promedio por tipo de cuenta", filas)

    # Consulta 3: top clientes por monto total
    filas = ejecutar_consulta(
        con,
        """
        SELECT cli.nombre, SUM(tr.monto) AS total
        FROM transacciones tr
        JOIN cuentas cu ON tr.id_cuenta = cu.id_cuenta
        JOIN clientes cli ON cu.id_cliente = cli.id_cliente
        GROUP BY cli.nombre
        ORDER BY total DESC
        LIMIT 5;
        """
    )
    mostrar_resultado("Top clientes por movimiento", filas)

    con.close()

    print("\nPipeline completado con éxito.")


if __name__ == "__main__":
    run_pipeline()
