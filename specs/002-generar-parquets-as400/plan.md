# Plan de Implementacion: Generacion de Parquets Simulando Data AS400

**Branch**: `002-generar-parquets-as400` | **Fecha**: 2026-04-03 | **Spec**: [spec.md](spec.md)
**Entrada**: Especificacion de feature desde `/specs/002-generar-parquets-as400/spec.md`

## Resumen

Crear 3 notebooks Python (.py) con formato Databricks que generen archivos parquet simulando la data de un sistema AS400 bancario: Maestro de Clientes (CMSTFL, 70 columnas), Transaccional (TRXPFL, 60 columnas) y Saldos (BLNCFL, 100 columnas). Los notebooks leen la configuracion de almacenamiento dinamico (Volume o S3) desde la tabla Parametros de Unity Catalog creada en el Incremento 1, reciben parametros especificos via `dbutils.widgets`, y escriben en formato parquet con modo overwrite. Se incluye un conjunto de pruebas TDD. Se usa Faker con fallback a listas estaticas para la generacion de datos.

## Contexto Tecnico

**Lenguaje/Version**: Python 3 / PySpark (Databricks Runtime Serverless)
**Dependencias Principales**: PySpark (nativo), Faker (con fallback a listas estaticas), dbutils, random (stdlib)
**Almacenamiento**: Unity Catalog Managed Volumes (primario) o Amazon S3 (secundario) — segun parametro TipoStorage
**Pruebas**: Notebooks TDD ejecutables en Databricks Serverless via extension VS Code
**Plataforma Destino**: Databricks Free Edition (serverless-only)
**Tipo de Proyecto**: Notebooks de generacion de datos (aislados del pipeline LSDP)
**Metas de Rendimiento**: Generar 50,000 clientes + 150,000 transacciones + 50,000 saldos en un entorno serverless con recursos limitados
**Restricciones**: Sin `spark.sparkContext`, sin acceso al JVM, particiones via parametro `numeroParticiones` (defecto 8), shuffle.partitions via parametro `shufflePartitions` (defecto 8), modo overwrite
**Escala/Alcance**: 3 notebooks generadores + 1 notebook TDD, ubicados en `src/LSDP_Laboratorio_Basico/explorations/GenerarParquets/`

## Verificacion de Constitution

*GATE: Debe pasar antes de la Fase 0. Re-verificar despues de la Fase 1.*

| Principio | Estado | Verificacion |
|-----------|--------|--------------|
| I. Plataforma Databricks Free Edition | PASA | Sin dependencias Azure. Uso exclusivo de Unity Catalog y Serverless |
| II. Storage Dinamico via TipoStorage | PASA | Los notebooks leen TipoStorage de la tabla Parametros y construyen rutas dinamicas (Volume o S3) |
| III. Idempotencia Diferenciada | PASA | Notebooks de generacion usan modelo de mutacion diferenciada (no idempotencia absoluta). TDD usa idempotencia absoluta |
| IV. Observabilidad desde el Minuto Cero | PASA | Cada notebook imprime parametros, rutas, conteos y tiempos |
| V. Parametrizacion Total | PASA | Todo via dbutils.widgets y tabla Parametros. Sin valores hardcodeados |
| VI. Compatibilidad Serverless | PASA | Sin spark.sparkContext ni acceso al JVM. Solo PySpark nativo |
| VII. Optimizacion Recursos Limitados | PASA | Particiones via `numeroParticiones` (defecto 8), shuffle via `shufflePartitions` (defecto 8), volumetria por defecto reducida |
| VIII. TDD | PASA | Incremento 2 incluye pruebas TDD obligatorias |
| IX. Paradigma LSDP | N/A | Este incremento NO crea scripts LSDP; solo genera parquets fuente |
| Restricciones Tecnologicas | PASA | Sin abfss://, /mnt/, DBFS root, Azure SQL, sparkContext, hardcoding |
| Estandares de Codigo | PASA | Espanol, snake_case, PascalCase con prefijo Nb, comentarios detallados |

**Resultado del Gate**: APROBADO — sin violaciones.

## Estructura del Proyecto

### Documentacion (esta feature)

```text
specs/002-generar-parquets-as400/
├── plan.md              # Este archivo
├── research.md          # Fase 0: investigacion
├── data-model.md        # Fase 1: modelo de datos
├── quickstart.md        # Fase 1: guia de inicio rapido
├── contracts/           # Fase 1: contratos
└── tasks.md             # Fase 2: tareas (generado por /speckit.tasks)
```

### Codigo Fuente (raiz del repositorio)

```text
src/
└── LSDP_Laboratorio_Basico/
    └── explorations/
        └── GenerarParquets/
            ├── NbGenerarMaestroCliente.py          # Generador CMSTFL (70 columnas)
            ├── NbGenerarTransaccionalCliente.py     # Generador TRXPFL (60 columnas)
            ├── NbGenerarSaldosCliente.py            # Generador BLNCFL (100 columnas)
            └── NbTddGenerarParquets.py              # Pruebas TDD para los 3 generadores
```

**Decision de Estructura**: Se usa la estructura definida en la seccion Plan del SYSTEM.md. Los notebooks de generacion de parquets residen en `explorations/GenerarParquets/` junto con su archivo de pruebas TDD. Las utilidades LSDP (LsdpConexionParametros.py, LsdpConstructorRutas.py) NO se crean en este incremento — la logica de lectura de parametros y construccion de rutas se implementa directamente dentro de cada notebook.

## Decisiones Tecnicas Aprobadas (Incremento 2)

Las siguientes decisiones fueron investigadas en el research de la Fase 0 y aprobadas
formalmente por el usuario el 2026-04-03. Rigen para la implementacion de este incremento.
Detalle completo en [research.md](research.md).

| # | Tema | Decision Aprobada |
|---|------|-------------------|
| 1 | Faker en Serverless | `try/except ImportError` con fallback a listas estaticas embebidas (~100 nombres, ~80 apellidos por etnia). Locales: `he_IL`, `ar_EG`, `en_US`/`en_GB` |
| 2 | Escritura de parquets | `df.coalesce(int(numeroParticiones)).write.mode("overwrite").parquet(ruta)` — parquets planos, no Delta. `numeroParticiones` recibido via widget (defecto 8) |
| 3 | Generacion de datos sin sparkContext | `spark.range(n)` + funciones nativas PySpark (`F.rand()`, `F.array()`, `F.element_at()`, etc.) |
| 4 | Lectura de tabla Parametros | `spark.read.table()` + `collect()` a diccionario Python |
| 5 | Mutacion diferenciada CMSTFL | Si `rutaMaestroClienteExistente` tiene valor, leer parquet existente, mutar `porcentajeMutacion` (defecto 0.20 = 20%) en campos de `camposMutacion` con `F.rand()` + `F.row_number()`, agregar `porcentajeNuevos` (defecto 0.006 = 0.6%) nuevos, union, overwrite en `rutaRelativaMaestroCliente` |
| 6 | Construccion de rutas | Logica inline `if/elif` en cada notebook (Volume vs S3). Utilidades LSDP se crean en Incremento 3 |
| 7 | Optimizacion Free Edition | `shufflePartitions` (defecto 8), `numeroParticiones` (defecto 8) via widgets, volumetria defecto 50K clientes / 150K transacciones, montos entre `montoMinimo` (10) y `montoMaximo` (100000) |

## Seguimiento de Complejidad

> Sin violaciones de constitution. No aplica.
