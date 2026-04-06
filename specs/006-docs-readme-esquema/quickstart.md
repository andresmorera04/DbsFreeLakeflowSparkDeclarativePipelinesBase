# Guia Rapida: Incremento 6 - Documentacion, README y Sustitucion de Esquemas

**Branch**: `006-docs-readme-esquema`
**Fecha**: 2026-04-06

---

## Prerrequisitos

- Incrementos 1-5 completamente implementados y funcionales
- Branch `006-docs-readme-esquema` creado y activo
- Acceso al repositorio con todos los archivos de `src/`, `conf/` y `specs/`

## Orden de Ejecucion

Este incremento es exclusivamente de documentacion y sustitucion de valores. No hay
notebooks ni scripts que ejecutar en Databricks. El orden de implementacion es:

### Paso 1: Sustitucion "regional" -> "lab1" en codigo (.py)

Sustituir todas las ocurrencias de "regional" usadas como valor de esquema en archivos .py:

| Directorio | Archivos | Ocurrencias |
|------------|----------|-------------|
| conf/ | NbConfiguracionInicial.py | 6 |
| src/.../GenerarParquets/ | NbGenerar*.py, NbTddGenerarParquets.py | 4 |
| src/.../LSDP_Laboratorio_Basico/ | NbTddBroncePipeline.py, NbTddOroPipeline.py | 6 |
| src/.../transformations/ | LsdpBronce*.py, LsdpPlata*.py, LsdpOroClientes.py | 12 |

**Verificacion**:
```bash
grep -rn "regional" --include="*.py" src/ conf/
# Debe retornar 0 resultados
```

### Paso 2: Sustitucion "regional" -> "lab1" en specs (.md)

Sustituir todas las ocurrencias en especificaciones de incrementos 001 a 005:

| Directorio | Archivos Afectados | Ocurrencias Estimadas |
|------------|-------------------|----------------------|
| specs/001-incremento1-config-inicial/ | data-model, contracts, quickstart, spec, tasks | ~14 |
| specs/002-generar-parquets-as400/ | contracts, quickstart | ~12 |
| specs/003-lsdp-bronce-pipeline/ | data-model, contracts, quickstart, spec, plan | ~15 |
| specs/004-lsdp-plata-vistas/ | data-model, contracts, quickstart, spec, plan | ~15 |
| specs/005-lsdp-oro-vistas/ | data-model, contracts, quickstart, spec, plan | ~15 |

**Verificacion**:
```bash
grep -rn "regional" --include="*.md" specs/00[1-5]*/
# Debe retornar 0 resultados en contexto de valor de esquema
```

**EXCLUIR**: SYSTEM.md y .specify/memory/constitution.md

### Paso 3: Crear directorio docs/ y generar ManualTecnico.md

Crear `docs/ManualTecnico.md` con las 7 secciones obligatorias:
1. Decoradores LSDP
2. Paradigma Declarativo
3. Propiedades Delta
4. Operaciones DataFrame
5. Parametros Pipeline/Notebooks
6. Tabla Parametros
7. Dependencias

**Verificacion**: El documento existe, tiene las 7 secciones, usa "lab1", esta en espanol.

### Paso 4: Generar ModeladoDatos.md

Crear `docs/ModeladoDatos.md` con diccionario de datos de las 10 entidades:
- Documentar TODOS los campos (~500+) con nombre, tipo, descripcion
- Incluir campos calculados con logica
- Incluir expectativas de calidad
- Incluir diagrama de linaje

**Verificacion**: Las 10 entidades estan documentadas, campos completos, usa "lab1".

### Paso 5: Actualizar README.md

Reescribir `README.md` con formato profesional (8 secciones minimo):
- Arquitectura, stack, estructura, guia rapida, documentacion, IA asistida
- Enlaces a ManualTecnico.md, ModeladoDatos.md, SYSTEM.md
- Mencion de GitHub Copilot y spec-kit

**Verificacion**: Formato profesional, 6+ secciones, enlaces correctos, mencion IA.

### Paso 6: Crear demo/ConfiguracionInicial.md

Crear `demo/ConfiguracionInicial.md` con 8 pasos:
1. Prerrequisitos (Databricks Free Edition)
2. Importar repositorio
3. Configurar extensiones VS Code
4. Ejecutar NbConfiguracionInicial.py
5. Generar datos de prueba
6. Crear pipeline LSDP
7. Ejecutar pipeline
8. Verificar resultados

**Verificacion**: Documento existe, 8 pasos, usa "lab1", pasos verificables.

### Paso 7: Verificacion final

```bash
# Verificar que "regional" no aparece en codigo
grep -rn "regional" --include="*.py" src/ conf/

# Verificar que "regional" no aparece en specs (excepto narrativas historicas)
grep -rn "regional" --include="*.md" specs/00[1-5]*/

# Verificar que los documentos existen
ls docs/ManualTecnico.md docs/ModeladoDatos.md demo/ConfiguracionInicial.md README.md
```

## Resultados Esperados

- `docs/ManualTecnico.md`: Manual tecnico completo (7 secciones)
- `docs/ModeladoDatos.md`: Diccionario de datos (~500 campos, 10 entidades)
- `demo/ConfiguracionInicial.md`: Guia de configuracion (8 pasos)
- `README.md`: Actualizado con formato profesional
- 0 ocurrencias de "regional" como valor de esquema en .py y specs
