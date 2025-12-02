
-- 1) Conteo básico de tablas

SELECT COUNT(*) AS total_clientes FROM clientes;
SELECT COUNT(*) AS total_cuentas FROM cuentas;
SELECT COUNT(*) AS total_transacciones FROM transacciones;


-- 2) Clientes activos vs inactivos

SELECT 
    activo,
    COUNT(*) AS cantidad
FROM clientes
GROUP BY activo;


-- 3) Saldo promedio por tipo de cuenta

SELECT 
    tipo_cuenta,
    AVG(saldo_inicial) AS saldo_promedio
FROM cuentas
GROUP BY tipo_cuenta;


-- 4) Total de transacciones por tipo

SELECT 
    tipo,
    COUNT(*) AS cantidad,
    SUM(monto) AS total_monto
FROM transacciones
GROUP BY tipo;


-- 5) JOIN: clientes + cuentas

SELECT 
    c.nombre,
    c.ciudad,
    cu.id_cuenta,
    cu.saldo_inicial
FROM clientes c
JOIN cuentas cu ON c.id_cliente = cu.id_cliente;


-- 6) JOIN completo con transacciones

SELECT 
    cli.nombre,
    cli.segmento,
    cue.tipo_cuenta,
    tr.monto,
    tr.tipo AS tipo_transaccion,
    tr.fecha
FROM transacciones tr
JOIN cuentas cue ON tr.id_cuenta = cue.id_cuenta
JOIN clientes cli ON cue.id_cliente = cli.id_cliente
ORDER BY tr.fecha;


-- 7) Total de movimientos por cliente

SELECT 
    cli.nombre,
    COUNT(tr.id_transaccion) AS total_movimientos,
    SUM(tr.monto) AS monto_total
FROM transacciones tr
JOIN cuentas cu ON tr.id_cuenta = cu.id_cuenta
JOIN clientes cli ON cu.id_cliente = cli.id_cliente
GROUP BY cli.nombre
ORDER BY monto_total DESC;


-- 8) Segmentación: cliente "alto movimiento"

SELECT 
    cli.nombre,
    SUM(tr.monto) AS monto_total,
    CASE 
        WHEN SUM(tr.monto) >= 20000 THEN 'alto'
        WHEN SUM(tr.monto) >= 5000 THEN 'medio'
        ELSE 'bajo'
    END AS categoria_movimiento
FROM transacciones tr
JOIN cuentas cu ON tr.id_cuenta = cu.id_cuenta
JOIN clientes cli ON cu.id_cliente = cli.id_cliente
GROUP BY cli.nombre;


-- 9) Días con más actividad

SELECT 
    DATE(fecha) AS dia,
    COUNT(*) AS cantidad_transacciones
FROM transacciones
GROUP BY DATE(fecha)
ORDER BY cantidad_transacciones DESC;


-- 10) Detectar transacciones sospechosas

SELECT *
FROM transacciones
WHERE monto IS NULL
    OR monto < 0
    OR monto > 1000000
    OR tipo IS NULL;
