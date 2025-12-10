-- contamos todas las filas de la tabla clientes
SELECT COUNT(*) AS total_clientes FROM clientes;
SELECT COUNT(*) AS total_cuentas FROM cuentas;
SELECT COUNT(*) AS total_transacciones FROM transacciones;


-- agrupamos los clientes por activo / inactivo
SELECT 
    activo,
    COUNT(*) AS cantidad
FROM clientes
GROUP BY activo;


-- consultamos el saldo promedio por tipo de cuenta cc, ca
SELECT 
    tipo_cuenta,
    AVG(saldo_inicial) AS saldo_promedio
FROM cuentas
GROUP BY tipo_cuenta;


-- agrupamos todas las transacciones segun el valor de la columna tipo
SELECT tipo, COUNT(*) AS cantidad, SUM(monto) AS total_monto
FROM transacciones
GROUP BY tipo;


-- unimos clientes con cuentas usando id_cliente para saber qué cuenta pertenece a cada cliente
SELECT c.nombre, c.ciudad, cu.id_cuenta, cu.saldo_inicial
FROM clientes c
JOIN cuentas cu ON c.id_cliente = cu.id_cliente;


-- unimos transacciones con cuentas y clientes para mostrar cada transacción con su cliente y ordenarla por fecha
SELECT  cli.nombre, cli.segmento, cue.tipo_cuenta, tr.monto, tr.tipo AS tipo_transaccion, tr.fecha
FROM transacciones tr
JOIN cuentas cue ON tr.id_cuenta = cue.id_cuenta
JOIN clientes cli ON cue.id_cliente = cli.id_cliente
ORDER BY tr.fecha;


-- aca unimos transacciones con cuentas y clientes para calcular, por cliente, cuántos movimientos hizo y el monto total movido
SELECT cli.nombre, COUNT(tr.id_transaccion) AS total_movimientos, SUM(tr.monto) AS monto_total
FROM transacciones tr
JOIN cuentas cu ON tr.id_cuenta = cu.id_cuenta
JOIN clientes cli ON cu.id_cliente = cli.id_cliente
GROUP BY cli.nombre
ORDER BY monto_total DESC;


-- calculamos el monto total por cliente y lo clasificamos en alto, medio o bajo según cuánto dinero movio
SELECT cli.nombre, SUM(tr.monto) AS monto_total,
    CASE 
        WHEN SUM(tr.monto) >= 20000 THEN 'alto'
        WHEN SUM(tr.monto) >= 5000 THEN 'medio'
        ELSE 'bajo'
    END AS categoria_movimiento
FROM transacciones tr
JOIN cuentas cu ON tr.id_cuenta = cu.id_cuenta
JOIN clientes cli ON cu.id_cliente = cli.id_cliente
GROUP BY cli.nombre;


-- de la columna fecha sacamos solo la fecha - contamos cuantas filas hay para cada dia
-- contamos cuántas transacciones hubo por día y ordenamos los días del más activo al menos activo
SELECT DATE(fecha) AS dia, COUNT(*) AS cantidad_transacciones
FROM transacciones
GROUP BY DATE(fecha) -- juntamos todas las filas que tengan el mismo dia en un solo grupo
ORDER BY cantidad_transacciones DESC;


-- Detectar transacciones sospechosas ,  solo las transacciones que tienen datos invalidos
SELECT * FROM transacciones
WHERE monto IS NULL
    OR monto < 0
    OR monto > 1000000
    OR tipo IS NULL;
