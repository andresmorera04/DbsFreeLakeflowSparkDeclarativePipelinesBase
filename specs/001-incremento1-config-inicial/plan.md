# Plan de Implementacion: Incremento 1 - Research Inicial y Configuracion Base

**Branch**: `001-incremento1-config-inicial` | **Fecha**: 2026-04-03 | **Spec**: [spec.md](spec.md)
**Entrada**: Especificacion de feature desde `/specs/001-incremento1-config-inicial/spec.md`

**Nota**: Este plan fue generado por el comando `/speckit.plan`. Ver `.specify/templates/plan-template.md` para el flujo de ejecucion.

## Resumen

El Incremento 1 establece la base del proyecto mediante: (1) investigacion inicial de Lakeflow Spark Declarative Pipelines (LSDP) y extensiones de Databricks para VS Code, (2) creacion del notebook de configuracion inicial `conf/NbConfiguracionInicial.py` que crea la tabla `Parametros` en Unity Catalog con 15 registros clave-valor, catalogos/esquemas automaticos y el Volume gestionado. Todo mediante `dbutils.widgets` con valores por defecto y con observabilidad completa.

## Contexto Tecnico

**Lenguaje/Version**: Python 3.x (PySpark via Databricks Runtime Serverless)
**Dependencias Principales**: PySpark, `dbutils` (nota: `databricks.sdk.pipelines` no aplica hasta Incremento 2+)
**Almacenamiento**: Unity Catalog Managed Volumes (por defecto) o Amazon S3 (via parametro TipoStorage)
**Entorno de Desarrollo**: VS Code con extensiones Databricks Extension y Databricks Driver for SQLTools verificadas. Ejecucion via "Run on Serverless" (Decision aprobada - research.md Tema 4).
**Pruebas**: Excluido para Incremento 1 (TDD aplica desde Incremento 2)
**Plataforma Destino**: Databricks Free Edition (serverless-only, cuotas limitadas)
**Tipo de Proyecto**: Pipeline de datos (ETL/ELT) con arquitectura medallion
**Metas de Rendimiento**: Notebook de configuracion ejecutable en menos de 2 minutos
**Restricciones**: Sin `spark.sparkContext`, sin `abfss://`, sin `/mnt/`, sin DBFS root, sin Azure, sin hardcoding, sin errores silenciosos
**Parametrizacion**: `dbutils.widgets.text()` con valores por defecto. Validar parametros al inicio del notebook antes de cualquier operacion (Decision aprobada - research.md Tema 5).
**Escala/Alcance**: 15 registros de parametros, 4 catalogos, 4 esquemas, 1 Volume gestionado

## Constitution Check

*GATE: Debe pasar antes de la investigacion Fase 0. Re-verificar despues del diseno Fase 1.*

| Principio | Estado | Verificacion |
|-----------|--------|-------------|
| I. Plataforma Exclusiva: Databricks Free Edition | PASA | El notebook solo usa Unity Catalog y Spark SQL nativos. Sin dependencias Azure. |
| II. Storage Dinamico via TipoStorage | PASA | La tabla Parametros incluye registros para Volume y S3. El notebook establece los parametros que lo habilitan. |
| III. Idempotencia Diferenciada | PASA | El notebook usa `CREATE OR REPLACE TABLE` + INSERT. La seccion `conf/` aplica idempotencia absoluta. |
| IV. Observabilidad desde el Minuto Cero | PASA | RF-005, RF-009, RF-010 exigen impresion de parametros, tiempos y resumen al inicio/fin. |
| V. Parametrizacion Total (No Hard-Coded) | PASA | Todos los valores via `dbutils.widgets` con valores por defecto. 15 registros parametrizados. |
| VI. Compatibilidad Maxima con Computo Serverless | PASA | El notebook usa solo `spark.sql()` y `dbutils`. Sin `sparkContext`. |
| VII. Optimizacion para Recursos Limitados | PASA | Operaciones livianas (DDL + 15 INSERTs). Sin procesamiento masivo de datos. |
| VIII. TDD | PASA | Incremento 1 esta explicitamente excluido del requisito de TDD. |
| IX. Paradigma Declarativo LSDP Estricto | N/A | Este incremento no crea pipelines LSDP. Solo establece la infraestructura base. |
| Restricciones Tecnologicas | PASA | Sin abfss://, sin /mnt/, sin DBFS, sin Azure SQL/KV, sin sparkContext, sin ZOrder/PartitionBy, sin hardcoding, sin errores silenciosos, sin emojis. |
| Estandares de Codigo | PASA | Notebook en espanol, PascalCase con prefijo "Nb", variables snake_case, comentarios detallados. |

**Resultado del Gate**: PASA (0 violaciones)

## Estructura del Proyecto

### Documentacion (este feature)

```text
specs/001-incremento1-config-inicial/
  plan.md              # Este archivo (salida de /speckit.plan)
  spec.md              # Especificacion del feature
  research.md          # Salida de Fase 0 (/speckit.plan)
  data-model.md        # Salida de Fase 1 (/speckit.plan)
  quickstart.md        # Salida de Fase 1 (/speckit.plan)
  contracts/           # Salida de Fase 1 (/speckit.plan)
  tasks.md             # Salida de Fase 2 (/speckit.tasks - NO creado por /speckit.plan)
```

### Codigo Fuente (raiz del repositorio)

```text
DbsFreeLakeflowSparkDeclarativePipelinesBase/
  README.md
  SYSTEM.md
  conf/
    NbConfiguracionInicial.py    # Crea catalogos, esquemas, tabla Parametros y Volume en UC
```

**Decision de Estructura**: Estructura minima para el Incremento 1. Solo se crea la carpeta `conf/` con el notebook de configuracion inicial. Los demas directorios (`src/`, `docs/`) se crean en incrementos posteriores.

## Seguimiento de Complejidad

> No hay violaciones al Constitution Check. Esta seccion no aplica.

## Re-verificacion Constitution Check Post-Diseno (Fase 1)

| Principio | Estado | Verificacion Post-Diseno |
|-----------|--------|--------------------------|
| I. Plataforma Exclusiva | PASA | El modelo de datos usa exclusivamente Unity Catalog DDL. Sin Azure. |
| II. Storage Dinamico | PASA | Tabla Parametros incluye 15 registros con parametros para Volume y S3 (registros nuevos: esquemaBronce, esquemaPlata, esquemaOro, esquemaControl). |
| III. Idempotencia | PASA | `CREATE OR REPLACE TABLE` para Parametros + `IF NOT EXISTS` para catalogos/esquemas/volume. Idempotencia absoluta confirmada. |
| IV. Observabilidad | PASA | Contrato define 5 bloques de salida por pantalla (resumen inicial, progreso, registros, tiempos, resumen final). |
| V. Parametrizacion Total | PASA | 3 widgets de entrada + 15 registros parametrizados. Cero valores hardcodeados. |
| VI. Serverless | PASA | Solo usa `spark.sql()` y `dbutils`. Sin `sparkContext`. Sin acceso al JVM. |
| VII. Recursos Limitados | PASA | Operaciones DDL livianas (15 INSERTs). Sin procesamiento masivo. |
| VIII. TDD | PASA | Incremento 1 excluido. |
| IX. LSDP | N/A | Sin pipelines en este incremento. |
| Estandares Codigo | PASA | Archivo PascalCase con prefijo "Nb", codigo en espanol, snake_case. |

**Resultado Post-Diseno**: PASA (0 violaciones). El diseno es consistente con la constitution.
