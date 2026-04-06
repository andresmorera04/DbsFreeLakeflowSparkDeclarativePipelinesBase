# Modelo de Datos: Incremento 6 - Documentacion, README y Sustitucion de Esquemas

**Branch**: `006-docs-readme-esquema`
**Fecha**: 2026-04-06

---

## Resumen

Este incremento no introduce entidades de datos nuevas en el pipeline. Los artefactos son exclusivamente documentos markdown y modificaciones de valores de configuracion en archivos existentes. El modelo de datos describe los documentos generados y el inventario de archivos afectados por la sustitucion "regional" -> "lab1".

---

## Artefactos de Documentacion (Nuevos)

### 1. docs/ManualTecnico.md

| Seccion | Contenido | Fuente de Datos |
|---------|-----------|-----------------|
| Decoradores LSDP | `@dp.table`, `@dp.materialized_view`, importacion, parametros, ejemplos | Archivos de transformations/ |
| Paradigma Declarativo | Patron Closure, cloudpickle, resolucion automatica de imports | SYSTEM.md, constitution.md |
| Propiedades Delta | CDF, autoOptimize, Liquid Cluster, retencion | Archivos de transformations/ |
| Operaciones DataFrame | readStream, read.table, joins, groupBy, agregaciones, F.when, F.sha2 | Archivos de transformations/ y utilities/ |
| Parametros Pipeline/Notebooks | 12 parametros LSDP + widgets de notebooks | SYSTEM.md, archivos .py |
| Tabla Parametros | 15 claves + TiposTransaccionesLabBase, estructura, ubicacion | conf/NbConfiguracionInicial.py |
| Dependencias | Databricks Free Edition, Unity Catalog, catalogos, esquemas, Volume, S3 | SYSTEM.md |

### 2. docs/ModeladoDatos.md

| Seccion | Contenido | Cantidad de Campos |
|---------|-----------|-------------------|
| Parquets AS400 | CMSTFL (70), TRXPFL (60), BLNCFL (100) — nombre, tipo PySpark, descripcion | 230 |
| Streaming Tables Bronce | cmstfl (75), trxpfl (65), blncfl (105) — columnas + 5 de control | 245 |
| Vistas Plata | clientes_saldos_consolidados (173), transacciones_enriquecidas (64) — mapeo + calculados | 237 |
| Vistas Oro | comportamiento_atm_cliente (6), resumen_integral_cliente (22) — metricas + seleccion | 28 |
| Tabla Parametros | Clave/Valor (15 registros + 1 especial) | 2 columnas x 16 registros |
| Diagrama de Linaje | Flujo AS400 -> Bronce -> Plata -> Oro con transformaciones | N/A |

**Total campos documentados**: ~580 (con duplicaciones por propagacion entre capas)

### 3. demo/ConfiguracionInicial.md

| Seccion | Contenido |
|---------|-----------|
| Prerrequisitos | Cuenta Databricks Free Edition, VS Code con extensiones |
| Paso 1 | Importar repositorio en workspace |
| Paso 2 | Configurar extensiones VS Code |
| Paso 3 | Ejecutar NbConfiguracionInicial.py |
| Paso 4 | Ejecutar notebooks generacion parquets |
| Paso 5 | Crear pipeline LSDP con parametros |
| Paso 6 | Ejecutar pipeline (bronce -> plata -> oro) |
| Paso 7 | Verificar resultados en vistas de oro |
| Paso 8 | Consultas de validacion |

### 4. README.md (Actualizacion)

| Seccion | Contenido |
|---------|-----------|
| Header | Titulo + descripcion concisa del proyecto |
| Descripcion | Proposito, plataforma, alcance |
| Arquitectura Medallion | Diagrama del flujo bronce -> plata -> oro |
| Stack Tecnologico | Tabla de componentes y tecnologias |
| Estructura del Proyecto | Arbol de directorios con descripciones |
| Guia Rapida | Pasos minimos para ejecutar |
| Documentacion | Enlaces a ManualTecnico.md, ModeladoDatos.md, SYSTEM.md |
| Desarrollo con IA Asistida | Mencion de GitHub Copilot y spec-kit |

---

## Inventario de Sustitucion "regional" -> "lab1"

### Archivos .py (Codigo Fuente)

| Archivo | Ocurrencias | Tipo de Cambio |
|---------|-------------|----------------|
| conf/NbConfiguracionInicial.py | 6 | Widget default (1) + INSERT values (5) |
| src/.../NbGenerarMaestroCliente.py | 1 | Widget default |
| src/.../NbGenerarSaldosCliente.py | 1 | Widget default |
| src/.../NbGenerarTransaccionalCliente.py | 1 | Widget default |
| src/.../NbTddGenerarParquets.py | 1 | Widget default |
| src/.../NbTddBroncePipeline.py | 4 | Datos esperados en asserts |
| src/.../NbTddOroPipeline.py | 2 | Valor de esquema en pruebas |
| src/.../LsdpBronceBlncfl.py | 2 | Comentarios magic %md |
| src/.../LsdpBronceCmstfl.py | 2 | Comentarios magic %md |
| src/.../LsdpBronceTrxpfl.py | 2 | Comentarios magic %md |
| src/.../LsdpOroClientes.py | 2 | Defaults en `.get()` |
| src/.../LsdpPlataClientesSaldos.py | 1 | Default en `.get()` |
| src/.../LsdpPlataTransacciones.py | 1 | Default en `.get()` |
| **Total** | **~26** | |

### Archivos .md (Especificaciones)

| Directorio | Archivos Afectados | Ocurrencias Estimadas |
|------------|-------------------|----------------------|
| specs/001-incremento1-config-inicial/ | data-model.md, contracts/, quickstart.md, spec.md, tasks.md | ~14 |
| specs/002-generar-parquets-as400/ | contracts/, quickstart.md | ~12 |
| specs/003-lsdp-bronce-pipeline/ | data-model.md, contracts/, quickstart.md, spec.md, plan.md | ~15 |
| specs/004-lsdp-plata-vistas/ | data-model.md, contracts/, quickstart.md, spec.md, plan.md | ~15 |
| specs/005-lsdp-oro-vistas/ | data-model.md, contracts/, quickstart.md, spec.md, plan.md | ~15 |
| **Total** | | **~80+** |

### Archivos EXCLUIDOS

| Archivo | Razon |
|---------|-------|
| SYSTEM.md | RF-018: Documento fuente de verdad, usuario lo actualiza manualmente |
| .specify/memory/constitution.md | Documento de gobernanza, contiene "regional" como referencia de la version original |

---

## Entidades de Datos del Proyecto (Referencia para ModeladoDatos.md)

Las 10 entidades del proyecto que deben documentarse en ModeladoDatos.md son:

| # | Entidad | Capa | Columnas | Tipo |
|---|---------|------|----------|------|
| 1 | CMSTFL | Fuente (Parquet AS400) | 70 | Archivo Parquet |
| 2 | TRXPFL | Fuente (Parquet AS400) | 60 | Archivo Parquet |
| 3 | BLNCFL | Fuente (Parquet AS400) | 100 | Archivo Parquet |
| 4 | bronce.lab1.cmstfl | Bronce | 75 | Streaming Table |
| 5 | bronce.lab1.trxpfl | Bronce | 65 | Streaming Table |
| 6 | bronce.lab1.blncfl | Bronce | 105 | Streaming Table |
| 7 | plata.lab1.clientes_saldos_consolidados | Plata | 173 | Vista Materializada |
| 8 | plata.lab1.transacciones_enriquecidas | Plata | 64 | Vista Materializada |
| 9 | oro.lab1.comportamiento_atm_cliente | Oro | 6 | Vista Materializada |
| 10 | oro.lab1.resumen_integral_cliente | Oro | 22 | Vista Materializada |

### Diagrama de Linaje

```
Parquets AS400 (CMSTFL, TRXPFL, BLNCFL)
    |  AutoLoader Streaming (rutas dinamicas: Volume o S3)
    v
BRONCE: Streaming Tables (append-only + FechaIngestaDatos)
    |  bronce.lab1.cmstfl (75 cols)
    |  bronce.lab1.trxpfl (65 cols)
    |  bronce.lab1.blncfl (105 cols)
    v
PLATA: Vistas Materializadas
    |  plata.lab1.clientes_saldos_consolidados (173 cols)
    |    <- LEFT JOIN cmstfl + blncfl, Dimension Tipo 1
    |    <- 4 campos calculados + 5 expectativas calidad
    |  plata.lab1.transacciones_enriquecidas (64 cols)
    |    <- SELECT sin filtros desde trxpfl (carga incremental)
    |    <- 4 campos calculados + 4 expectativas calidad
    v
ORO: Vistas Materializadas (producto de datos final)
    |  oro.lab1.comportamiento_atm_cliente (6 cols)
    |    <- groupBy identificador_cliente desde transacciones
    |    <- 5 metricas ATM (DATM, CATM, PGSL)
    |  oro.lab1.resumen_integral_cliente (22 cols)
    |    <- INNER JOIN plata.clientes_saldos + oro.comportamiento_atm
```

### Campos Calculados

| Capa | Entidad | Campo | Logica |
|------|---------|-------|--------|
| Plata | clientes_saldos_consolidados | clasificacion_riesgo_cliente | CASE sobre score_cliente, nivel_riesgo, calificacion_crediticia |
| Plata | clientes_saldos_consolidados | categoria_saldo_disponible | CASE sobre saldo_disponible, limite_credito |
| Plata | clientes_saldos_consolidados | perfil_actividad_bancaria | CASE sobre cantidad_transacciones, cantidad_cuentas, ranking_prestamos |
| Plata | clientes_saldos_consolidados | huella_identificacion_cliente | SHA2-256 de identificador_cliente |
| Plata | transacciones_enriquecidas | monto_neto_comisiones | monto_principal - comision_transaccion |
| Plata | transacciones_enriquecidas | porcentaje_comision_sobre_monto | (comision / monto) * 100, con proteccion div/0 |
| Plata | transacciones_enriquecidas | variacion_saldo_transaccion | saldo_posterior - saldo_anterior |
| Plata | transacciones_enriquecidas | indicador_impacto_financiero | monto_principal * riesgo_transaccion |
| Oro | comportamiento_atm_cliente | cantidad_depositos_atm | COUNT WHERE tipo_transaccion = DATM |
| Oro | comportamiento_atm_cliente | cantidad_retiros_atm | COUNT WHERE tipo_transaccion = CATM |
| Oro | comportamiento_atm_cliente | promedio_monto_depositos_atm | AVG(monto_principal) WHERE DATM |
| Oro | comportamiento_atm_cliente | promedio_monto_retiros_atm | AVG(monto_principal) WHERE CATM |
| Oro | comportamiento_atm_cliente | total_pagos_saldo_cliente | SUM(monto_principal) WHERE PGSL |

### Expectativas de Calidad de Datos

| Vista | Expectativa | Regla |
|-------|-------------|-------|
| clientes_saldos_consolidados | limite_credito_positivo | limite_credito > 0 |
| clientes_saldos_consolidados | identificador_cliente_no_nulo | identificador_cliente IS NOT NULL |
| clientes_saldos_consolidados | limite_credito_no_nulo | limite_credito IS NOT NULL |
| clientes_saldos_consolidados | fecha_apertura_valida | fecha_apertura_cuenta > 2020-12-31 |
| clientes_saldos_consolidados | fecha_nacimiento_valida | fecha_nacimiento < 2009-01-01 |
| transacciones_enriquecidas | moneda_no_nula | moneda_transaccion IS NOT NULL |
| transacciones_enriquecidas | monto_neto_no_nulo | monto_neto IS NOT NULL |
| transacciones_enriquecidas | monto_neto_positivo | monto_neto > 0 |
| transacciones_enriquecidas | identificador_cliente_no_nulo | identificador_cliente IS NOT NULL |
