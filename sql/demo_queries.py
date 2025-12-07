import sqlite3
from pathlib import Path


# Ejecución de consultas SQL de muestr
def consulta_sql():
    print("→ Ejecutando consultas demo en SQLite...\n")
    BASE = Path(__file__).resolve().parent.parent
    con = sqlite3.connect(BASE / "db" / "banco.db")

    def query(sql):
        return con.execute(sql).fetchall()

    # consulta sql
    total_de_clientes = query("SELECT COUNT(*) FROM clientes;")
    saldo_promedio_por_tipo = query("SELECT tipo_cuenta, AVG(saldo_inicial) FROM cuentas GROUP BY tipo_cuenta;")
    top_movimientos = query("""SELECT cli.nombre, SUM(tr.monto) AS total
                        FROM transacciones tr
                        JOIN cuentas cu ON tr.id_cuenta = cu.id_cuenta
                        JOIN clientes cli ON cu.id_cliente = cli.id_cliente
                        GROUP BY cli.nombre
                        ORDER BY total DESC
                        LIMIT 5;""")
    con.close()

    print(f'Total de clientes: {int(total_de_clientes[0][0])}')
    print("Saldo Promedio por tipo:")
    for tipo, saldo in saldo_promedio_por_tipo:
        print(f"- {tipo}: {saldo:.2f}")

    print("Top de movimientos:")
    for nombre, total in top_movimientos:
        print(f"- {nombre.title()}: {total:.2f}")




