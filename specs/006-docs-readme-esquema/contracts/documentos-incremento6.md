# Contrato: Documentos de Documentacion Incremento 6

**Branch**: `006-docs-readme-esquema`
**Fecha**: 2026-04-06

---

## Descripcion

Este contrato define la estructura, secciones obligatorias y criterios de validacion
para cada documento generado en el Incremento 6. Dado que este incremento no expone
interfaces externas (APIs, CLI, endpoints), el contrato describe los artefactos de
documentacion como la interfaz del usuario con el proyecto.

---

## 1. docs/ManualTecnico.md

### Estructura Obligatoria (7 secciones — RF-001 a RF-007)

| # | Seccion | Requisito | Validacion |
|---|---------|-----------|------------|
| 1 | Decoradores LSDP | Documentar `@dp.table` y `@dp.materialized_view` con ejemplos reales del proyecto | Verificar que los ejemplos coinciden con el codigo implementado |
| 2 | Paradigma Declarativo | Patron Closure, cloudpickle, resolucion de imports, motor declarativo | Verificar consistencia con codigo de transformations/ |
| 3 | Propiedades Delta | CDF, autoOptimize, Liquid Cluster, retencion (30d/60d) | Verificar que las propiedades coinciden con las definidas en decoradores |
| 4 | Operaciones DataFrame | readStream, read.table, joins, groupBy, agregaciones, F.when, F.sha2 | Verificar que las operaciones documentadas existen en el codigo |
| 5 | Parametros Pipeline/Notebooks | 12 parametros LSDP + widgets de cada notebook | Verificar que el conteo y nombres coinciden con SYSTEM.md |
| 6 | Tabla Parametros | 15 claves + TiposTransaccionesLabBase, con descripcion de uso | Verificar contra conf/NbConfiguracionInicial.py actualizado |
| 7 | Dependencias | Databricks Free Edition, Unity Catalog, catalogos, esquemas, Volume, S3 | Verificar que no mencione Azure ni servicios prohibidos |

### Restricciones

- Idioma: espanol (RF-022)
- Valores de esquema: "lab1" (RF-021)
- Sin emojis (constitution)
- Ejemplos de codigo deben ser extractos reales del proyecto, no genericos

---

## 2. docs/ModeladoDatos.md

### Estructura Obligatoria (RF-008 a RF-010)

| # | Seccion | Requisito | Validacion |
|---|---------|-----------|------------|
| 1 | Resumen de Entidades | Tabla con las 10 entidades: nombre, capa, cantidad columnas | Verificar que son exactamente 10 entidades |
| 2 | Parquets AS400 (3 entidades) | CMSTFL (70), TRXPFL (60), BLNCFL (100) — todos los campos | Verificar contra codigo de NbGenerar*.py |
| 3 | Streaming Tables Bronce (3 entidades) | cmstfl (75), trxpfl (65), blncfl (105) | Verificar contra codigo de LsdpBronce*.py |
| 4 | Vistas Plata (2 entidades) | clientes_saldos (173), transacciones (64) | Verificar contra LsdpPlata*.py |
| 5 | Vistas Oro (2 entidades) | comportamiento_atm (6), resumen_integral (22) | Verificar contra LsdpOroClientes.py |
| 6 | Campos Calculados | 13 campos con logica detallada | Verificar que la logica coincide con F.when/F.sha2 del codigo |
| 7 | Expectativas de Calidad | 9 expectativas (5 plata clientes + 4 plata transacciones) | Verificar contra decoradores en Plata |
| 8 | Diagrama de Linaje | Flujo AS400 -> Bronce -> Plata -> Oro con transformaciones | Verificar que refleja el flujo real del pipeline |

### Formato por Entidad

Cada entidad debe documentar:
- Nombre completo (3 partes para tablas UC)
- Capa del medallion
- Tipo (Parquet / Streaming Table / Vista Materializada)
- Cantidad de columnas
- Tabla con TODOS los campos: nombre, tipo de dato, descripcion
- Propiedades (Liquid Cluster, Delta properties) donde aplique
- Logica de campos calculados donde aplique

### Restricciones

- TODOS los campos de las 10 entidades deben estar documentados (~500+ campos) — clarificacion Q1
- Idioma: espanol (RF-022)
- Valores de esquema: "lab1" (RF-021)
- Sin emojis (constitution)

---

## 3. README.md

### Estructura Obligatoria (RF-011 a RF-014)

| # | Seccion | Requisito | Validacion |
|---|---------|-----------|------------|
| 1 | Header + Descripcion | Titulo profesional, descripcion concisa | Verificar formato profesional |
| 2 | Arquitectura Medallion | Diagrama del flujo bronce -> plata -> oro | Verificar que refleja la arquitectura real |
| 3 | Stack Tecnologico | Tabla con componentes y tecnologias | Verificar contra SYSTEM.md |
| 4 | Estructura del Proyecto | Arbol de directorios con descripciones | Verificar contra estructura real |
| 5 | Guia Rapida | Pasos minimos para ejecutar el proyecto | Verificar que los pasos son ejecutables |
| 6 | Documentacion | Enlaces a ManualTecnico.md, ModeladoDatos.md, SYSTEM.md | Verificar que los enlaces son correctos |
| 7 | Desarrollo con IA Asistida | Mencion de GitHub Copilot y spec-kit | Verificar mencion explicita (RF-013) |
| 8 | Creditos/Licencia | Informacion de atribucion | Opcional |

### Restricciones

- Formato profesional al nivel de Netflix, Spotify, Uber, Microsoft, Amazon (RF-011)
- Idioma: espanol (RF-022)
- Sin emojis (constitution)
- Debe enlazar a `docs/ManualTecnico.md`, `docs/ModeladoDatos.md` y `SYSTEM.md` (RF-014)

---

## 4. demo/ConfiguracionInicial.md

### Estructura Obligatoria (RF-019 a RF-020)

| # | Seccion | Requisito | Validacion |
|---|---------|-----------|------------|
| 1 | Prerrequisitos | Cuenta Databricks Free Edition, VS Code, extensiones | Verificar completitud |
| 2 | Importar repositorio | Pasos para importar desde GitHub | Verificar que los pasos son claros |
| 3 | Configurar extensiones | Pasos para Databricks Extension + SQLTools | Verificar que son ejecutables |
| 4 | Ejecutar configuracion inicial | NbConfiguracionInicial.py con parametros | Verificar parametros correctos |
| 5 | Generar datos de prueba | Ejecutar notebooks de GenerarParquets/ | Verificar orden de ejecucion |
| 6 | Crear pipeline LSDP | Configurar pipeline con 12 parametros | Verificar que lista los 12 parametros |
| 7 | Ejecutar pipeline | Primera ejecucion bronce -> plata -> oro | Verificar secuencia |
| 8 | Verificar resultados | Consultas SQL para validar vistas de oro | Verificar que las consultas son correctas |

### Restricciones

- Minimo 5 pasos secuenciales (CE-005) — el contrato define 8
- Idioma: espanol (RF-022)
- Valores de esquema: "lab1" (RF-021)
- Sin emojis (constitution)

---

## 5. Sustitucion "regional" -> "lab1"

### Contrato de Transformacion

| Aspecto | Regla |
|---------|-------|
| Alcance codigo | Todos los archivos .py en src/ y conf/ |
| Alcance specs | Todos los archivos .md en specs/001 a specs/005 |
| Exclusiones | SYSTEM.md, .specify/memory/constitution.md |
| Contexto valido | Valores de esquema de Unity Catalog unicamente (RF-017) |
| Contexto invalido | Comentarios genericos, narrativas historicas |
| Verificacion | `grep -rn "regional" --include="*.py" src/ conf/` debe retornar 0 resultados |
| Verificacion specs | `grep -rn "regional" specs/00[1-5]*/` debe retornar 0 resultados (valores de esquema) |
