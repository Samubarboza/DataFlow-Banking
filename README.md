# **README — Sistema ETL con Microservicios de Normalización y Reglas**

## **Resumen del Proyecto**

Este proyecto implementa un **pipeline ETL completo**, acompañado de dos **microservicios independientes** para proveer:

* **Catálogos y normalizaciones estándar**
* **Reglas de validación y riesgo**

El sistema permite procesar datos bancarios (clientes, cuentas y transacciones), aplicar transformaciones basadas en reglas dinámicas, generar datasets limpios y producir resultados en múltiples formatos (CSV/JSON/Parquet), además de cargarlos en una base de datos SQLite.

Todo el sistema está diseñado con **arquitectura modular**, **logging en SQLite** y separación total entre el pipeline ETL y los microservicios.

---

## **Arquitectura General**

``
root/
│
├── etl/                     # Lógica del pipeline (Extract / Transform / Load)
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   └── utils.py             # Logger SQLite del ETL
│
├── microservices/
│   ├── catalog_service/     # Microservicio 1: Catálogos
│   │   ├── app.py
│   │   ├── routes/
│   │   ├── services/
│   │   ├── config/
│   │   └── utils/logger.py  # Logger SQLite independiente
│   │
│   └── risk_rules_service/  # Microservicio 2: Reglas de riesgo
│       ├── app.py
│       ├── routes/
│       ├── services/
│       ├── config/
│       └── utils/logger.py  # Logger SQLite independiente
│
├── data/
│   ├── raw/                 # Datos originales
│   ├── processed/           # Datos limpios y datos con errores
│   └── outputs/             # Resultados finales del pipeline
│
├── db/
│   └── banco.db             # Base de datos final del modelo
│
└── run_pipeline.py          # Script principal del ETL
``

---

## **Flujo de Funcionamiento**

El sistema se ejecuta en **tres etapas principales**, siguiendo el patrón estándar ETL:

---

## **1. Extract (EXTRAER)**

El ETL:

1. Carga los archivos CSV principales:

   * clientes.csv
   * cuentas.csv
   * transacciones.csv

2. Consulta a los microservicios:

   * `catalog_service` → catálogos, normalización y mapas de valores
   * `risk_rules_service` → reglas de validación de montos, fechas y edades

3. Combina datos + reglas en un único objeto `data` listo para transformar.

**Importante:**
Los microservicios *no proveen datos nuevos*, sino **reglas** para procesar los datos del CSV.

---

## **2. Transform (TRANSFORMAR)**

En esta fase se aplican:

* Normalizaciones basadas en catálogos
* Validaciones dinámicas basadas en reglas
* Correcciones de tipos
* Limpieza de inconsistencias
* Manejo de nulos
* Ajustes según el dominio bancario

La transformación produce dos resultados por cada dataset:

### ✔️ Datos limpios

Ejemplo:

``
clientes_clean.csv
cuentas_clean.csv
transacciones_clean.csv
``

### ✔️ Datos rechazados por error

Ejemplo:

``
clientes_errores.csv
cuentas_errores.csv
transacciones_errores.csv
``

Estos archivos sirven para auditoría y control de calidad.

---

## **3. Load (CARGAR)**

El ETL:

1. Inserta los datos limpios en la base `banco.db`
2. Genera formatos finales:

   * CSV
   * JSON
   * Parquet
3. Ejecuta consultas SQL de verificación (demo)

---

## **Microservicios**

## **Catalog Service**

Provee:

* Mapas de normalización
* Catálogos corregidos
* Categorías válidas
* Valores permitidos

Endpoints típicos:

``
/catalogos
/normalizacion
``

---

## **Risk Rules Service**

Provee reglas de validación:

* Fechas válidas
* Rango permitido para montos
* Reglas de edad
* Monedas aceptadas

Endpoints típicos:

``
/reglas_monto
/reglas_edad
/reglas_fecha
/monedas_validas
``

---

## **Logging (SQLite)**

Cada componente escribe sus logs en su propia base de datos:

| Componente         | Archivo SQLite                         |
| ------------------ | -------------------------------------- |
| ETL                | `logs/etl_logs.db`                     |
| Catalog Service    | `catalog_service/logs/catalog_logs.db` |
| Risk Rules Service | `risk_rules_service/logs/risk_logs.db` |

Esto permite auditoría, consultas y dashboards sobre logs.

Ejemplo de consulta:

```sql
SELECT * FROM logs ORDER BY id DESC;
```

---

## **Ejecución del Proyecto**

## **1) Levantar microservicios**

En terminal 1:

``
cd microservices/catalog_service
python app.py run
``

En terminal 2:

``
cd microservices/risk_rules_service
python app.py run
``

---

## **2) Ejecutar ETL**

Desde la raíz del proyecto:

``
python run_pipeline.py
``

El pipeline:

* consulta microservicios
* procesa datos
* guarda outputs
* carga la DB
* registra logs en SQLite

---

## **Estado del Proyecto**

Este proyecto está **en desarrollo activo**.
La estructura, arquitectura y componentes principales están definidos, pero seguirán expandiéndose:

* Nuevos endpoints
* Nuevas reglas
* Dashboard de logs
* Métricas de calidad de datos
* Test unitarios
* Integración CI/CD

El README se actualizará conforme avance el desarrollo.

---

## **Licencia / Autor / Uso**
