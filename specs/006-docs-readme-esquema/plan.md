# Plan de Implementacion: Incremento 6 - Documentacion, README y Sustitucion de Esquemas

**Branch**: `006-docs-readme-esquema` | **Fecha**: 2026-04-06 | **Spec**: [specs/006-docs-readme-esquema/spec.md](../spec.md)
**Entrada**: Especificacion de feature desde `/specs/006-docs-readme-esquema/spec.md`

## Resumen

Este incremento genera la documentacion tecnica completa del proyecto (ManualTecnico.md, ModeladoDatos.md), actualiza el README.md con formato profesional de empresas de referencia, sustituye todas las ocurrencias de "regional" por "lab1" como valor de esquema de Unity Catalog en codigo y especificaciones, y crea la guia de configuracion inicial en `demo/ConfiguracionInicial.md`. No hay codigo funcional nuevo — el entregable es exclusivamente documentos markdown y modificaciones de valores de configuracion en archivos existentes.

## Contexto Tecnico

**Lenguaje/Version**: Python 3.x / PySpark (Databricks Runtime)
**Dependencias Principales**: Databricks Free Edition, Unity Catalog, LSDP (`pyspark.pipelines`)
**Almacenamiento**: Unity Catalog Managed Volumes / Amazon S3 (segun TipoStorage)
**Testing**: N/A para este incremento (entregas de documentacion). Verificacion via busqueda exhaustiva de "regional" post-sustitucion
**Plataforma Destino**: Databricks Free Edition (serverless-only)
**Tipo de Proyecto**: Pipeline de datos con arquitectura medallion (Bronce/Plata/Oro)
**Objetivos de Rendimiento**: N/A (incremento de documentacion)
**Restricciones**: Idioma espanol obligatorio en toda la documentacion generada. "lab1" como valor de esquema en toda la documentacion
**Alcance/Ambito**: 4 documentos nuevos (ManualTecnico.md, ModeladoDatos.md, ConfiguracionInicial.md, README actualizado) + sustitucion transversal "regional" -> "lab1" en 33 ocurrencias en .py y ~80+ ocurrencias en specs .md

## Constitution Check (Pre-Diseno)

*GATE: Debe pasar antes de Phase 0 research. Se re-evalua despues de Phase 1 design.*

| Principio | Aplica | Estado | Notas |
|-----------|--------|--------|-------|
| I. Plataforma Exclusiva Databricks Free Edition | Si | PASS | Toda la documentacion describe Databricks Free Edition. Sin referencia a Azure |
| II. Storage Dinamico via TipoStorage | Si | PASS | Documentacion debe describir ambos modos (Volume y AmazonS3) |
| III. Idempotencia Diferenciada | No | N/A | Incremento de documentacion, no hay codigo funcional |
| IV. Observabilidad desde el Minuto Cero | No | N/A | No hay codigo ejecutable nuevo |
| V. Parametrizacion Total (No Hard-Coded) | Si | PASS | La sustitucion "regional"->"lab1" actualiza valores por defecto, los parametros siguen siendo dinamicos |
| VI. Compatibilidad Maxima con Serverless | No | N/A | No hay codigo funcional nuevo |
| VII. Optimizacion para Recursos Limitados | No | N/A | No hay codigo funcional nuevo |
| VIII. Test-Driven Development (TDD) | Si | PASS | Spec declara explicitamente que TDD no aplica para este incremento (entregas markdown). Se justifica en la nota del spec |
| IX. Paradigma Declarativo LSDP Estricto | Si | PASS | La documentacion debe describir correctamente los decoradores y el paradigma |
| Restricciones Tecnologicas | Si | PASS | Documentacion no introduce uso prohibido alguno |
| Flujo de Desarrollo y Estandares | Si | PASS | Documentacion en espanol, formato profesional |

**Resultado Gate Pre-Diseno: PASS (11/11)**

## Estructura del Proyecto

### Documentacion (este feature)

```text
specs/006-docs-readme-esquema/
├── plan.md              # Este archivo (salida de /speckit.plan)
├── research.md          # Salida de Phase 0 (/speckit.plan)
├── data-model.md        # Salida de Phase 1 (/speckit.plan)
├── quickstart.md        # Salida de Phase 1 (/speckit.plan)
├── contracts/           # Salida de Phase 1 (/speckit.plan)
└── tasks.md             # Salida de Phase 2 (/speckit.tasks - NO creado por /speckit.plan)
```

### Codigo Fuente y Documentacion (raiz del repositorio)

```text
DbsFreeLakeflowSparkDeclarativePipelinesBase/
├── README.md                           # MODIFICAR: Actualizar con formato profesional
├── SYSTEM.md                           # NO MODIFICAR (excluido de sustitucion automatica)
├── conf/
│   └── NbConfiguracionInicial.py       # MODIFICAR: Sustituir "regional" -> "lab1" (6 ocurrencias)
├── docs/                               # CREAR: Nuevo directorio
│   ├── ManualTecnico.md                # CREAR: Manual tecnico completo (7 secciones)
│   └── ModeladoDatos.md                # CREAR: Diccionario de datos (10 entidades, ~500 campos)
├── demo/                               # CREAR: Nuevo directorio
│   └── ConfiguracionInicial.md         # CREAR: Guia paso a paso
├── src/
│   └── LSDP_Laboratorio_Basico/
│       ├── explorations/
│       │   ├── GenerarParquets/
│       │   │   ├── NbGenerarMaestroCliente.py       # MODIFICAR: "regional" -> "lab1" (1)
│       │   │   ├── NbGenerarSaldosCliente.py        # MODIFICAR: "regional" -> "lab1" (1)
│       │   │   ├── NbGenerarTransaccionalCliente.py # MODIFICAR: "regional" -> "lab1" (1)
│       │   │   └── NbTddGenerarParquets.py          # MODIFICAR: "regional" -> "lab1" (1)
│       │   └── LSDP_Laboratorio_Basico/
│       │       ├── NbTddBroncePipeline.py           # MODIFICAR: "regional" -> "lab1" (4)
│       │       └── NbTddOroPipeline.py              # MODIFICAR: "regional" -> "lab1" (2)
│       ├── transformations/
│       │   ├── LsdpBronceBlncfl.py                  # MODIFICAR: "regional" -> "lab1" en comentarios (2)
│       │   ├── LsdpBronceCmstfl.py                  # MODIFICAR: "regional" -> "lab1" en comentarios (2)
│       │   ├── LsdpBronceTrxpfl.py                  # MODIFICAR: "regional" -> "lab1" en comentarios (2)
│       │   ├── LsdpOroClientes.py                   # MODIFICAR: "regional" -> "lab1" en defaults (2+)
│       │   ├── LsdpPlataClientesSaldos.py           # MODIFICAR: "regional" -> "lab1" en defaults (1+)
│       │   └── LsdpPlataTransacciones.py            # MODIFICAR: "regional" -> "lab1" en defaults (1+)
│       └── utilities/                               # SIN CAMBIOS
└── specs/
    ├── 001-incremento1-config-inicial/              # MODIFICAR: "regional" -> "lab1" en specs
    ├── 002-generar-parquets-as400/                  # MODIFICAR: "regional" -> "lab1" en specs
    ├── 003-lsdp-bronce-pipeline/                    # MODIFICAR: "regional" -> "lab1" en specs
    ├── 004-lsdp-plata-vistas/                       # MODIFICAR: "regional" -> "lab1" en specs
    ├── 005-lsdp-oro-vistas/                         # MODIFICAR: "regional" -> "lab1" en specs
    └── 006-docs-readme-esquema/                     # ESTE INCREMENTO
```

**Decision de Estructura**: Se crean 2 directorios nuevos (`docs/` y `demo/`) en la raiz del repositorio. Los archivos de documentacion se generan en estos directorios. La sustitucion "regional" -> "lab1" es transversal a todo el repositorio (excepto SYSTEM.md).

## Seguimiento de Complejidad

> No hay violaciones de la constitution que requieran justificacion.
> La excepcion de TDD esta documentada y justificada en el spec (entregas exclusivamente markdown).

---

## Constitution Check (Post-Diseno)

*Re-evaluacion tras completar Phase 0 (research.md) y Phase 1 (data-model.md, contracts/, quickstart.md)*

| Principio | Aplica | Estado | Notas |
|-----------|--------|--------|-------|
| I. Plataforma Exclusiva Databricks Free Edition | Si | PASS | Toda documentacion generada describe Databricks Free Edition. Sin Azure. ManualTecnico incluye seccion de dependencias correcta |
| II. Storage Dinamico via TipoStorage | Si | PASS | ManualTecnico documenta ambos modos (Volume/S3). ConfiguracionInicial.md usa Volume como defecto |
| III. Idempotencia Diferenciada | No | N/A | Incremento de documentacion |
| IV. Observabilidad desde el Minuto Cero | No | N/A | No hay codigo ejecutable nuevo |
| V. Parametrizacion Total (No Hard-Coded) | Si | PASS | Sustitucion "regional"->"lab1" modifica valores por defecto de parametros. Los parametros siguen siendo dinamicos via widgets/pipeline params |
| VI. Compatibilidad Maxima con Serverless | No | N/A | No hay codigo funcional nuevo |
| VII. Optimizacion para Recursos Limitados | No | N/A | No hay codigo funcional nuevo |
| VIII. Test-Driven Development (TDD) | Si | PASS | Justificado: entregas exclusivamente markdown. No hay utilidades nuevas testeables |
| IX. Paradigma Declarativo LSDP Estricto | Si | PASS | ManualTecnico documenta correctamente decoradores, patron Closure, imports automaticos. ModeladoDatos describe las 10 entidades del pipeline |
| Restricciones Tecnologicas | Si | PASS | Ninguna documentacion introduce uso prohibido. SYSTEM.md excluido de sustitucion (RF-018) |
| Flujo de Desarrollo y Estandares | Si | PASS | Toda documentacion en espanol. Sin emojis. README profesional. Esquemas actualizados a "lab1" |

**Resultado Gate Post-Diseno: PASS (11/11)**

**Validaciones adicionales post-diseno**:
- El research.md resuelve las 5 areas de investigacion sin NEEDS CLARIFICATION pendientes
- El data-model.md documenta las 10 entidades con ~580 campos y diagrama de linaje
- El contrato define estructura obligatoria para cada documento con criterios de validacion
- El quickstart.md establece 7 pasos de implementacion con verificaciones explicitas
- No se detectaron conflictos entre el diseno y los principios de la constitution
