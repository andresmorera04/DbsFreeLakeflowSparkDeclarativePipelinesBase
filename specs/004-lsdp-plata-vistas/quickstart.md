# Guia Rapida: Incremento 4 - LSDP Medalla de Plata - Vistas Materializadas

**Feature**: 004-lsdp-plata-vistas
**Fecha**: 2026-04-05

## Prerequisitos

1. **Incrementos anteriores completados**:
   - Inc. 1: Tabla Parametros creada en Unity Catalog con todas las claves, incluyendo `catalogoBronce`, `catalogoPlata`, `esquemaPlata`, `catalogoOro`.
   - Inc. 2: Parquets AS400 generados (CMSTFL, TRXPFL, BLNCFL).
   - Inc. 3: Pipeline LSDP con medalla de bronce funcional. Streaming tables `bronce.lab1.cmstfl`, `bronce.lab1.trxpfl`, `bronce.lab1.blncfl` pobladas.

2. **Entorno Databricks**:
   - Databricks Free Edition con Serverless Compute disponible.
   - Catalogos `plata` y esquema `lab1` creados por `conf/NbConfiguracionInicial.py`.
   - Extension Databricks para VS Code configurada y conectada al workspace.

## Estructura de Archivos Nuevos

```text
src/LSDP_Laboratorio_Basico/transformations/
├── LsdpPlataClientesSaldos.py     # Vista materializada clientes_saldos_consolidados
└── LsdpPlataTransacciones.py      # Vista materializada transacciones_enriquecidas
```

## Configuracion del Pipeline LSDP

Los scripts de plata se agregan al mismo pipeline LSDP existente del Incremento 3. El pipeline debe tener configurados:

**Parametros del pipeline** (ya existentes desde Inc. 3):

| Parametro | Valor (ejemplo) |
|-----------|----------------|
| catalogoParametro | control |
| esquemaParametro | lab1 |
| tablaParametros | Parametros |

**Catalogo y esquema destino de plata**: El pipeline LSDP se configura con el catalogo por defecto `bronce` y esquema `lab1`. Las vistas materializadas de plata se crean en `plata.lab1` — LSDP permite crear tablas/vistas en catalogos diferentes al por defecto.

## Flujo de Desarrollo

### Paso 1 — Verificar prerequisitos de bronce

Ejecutar el pipeline LSDP existente y confirmar que las 3 streaming tables de bronce contienen datos.

### Paso 2 — Implementar LsdpPlataClientesSaldos.py

1. Crear el archivo siguiendo el patron Closure (ver bronc como referencia).
2. Implementar el mapeo de columnas AS400 → espanol snake_case (ver data-model.md).
3. Implementar Dimension Tipo 1 con Window + ROW_NUMBER.
4. Implementar LEFT JOIN por CUSTID.
5. Implementar 4 campos calculados.
6. Configurar 5 `@dp.expect` y propiedades Delta.

### Paso 3 — Implementar LsdpPlataTransacciones.py

1. Crear el archivo siguiendo el patron Closure simplificado.
2. Implementar el mapeo de columnas AS400 → espanol snake_case.
3. Implementar 4 campos calculados numericos.
4. Configurar 4 `@dp.expect` y propiedades Delta.
5. Verificar que NO hay filtros ni funciones no deterministas.

### Paso 4 — TDD (si aplica)

Si se crean utilidades nuevas en `utilities/`, implementar pruebas en `NbTddPlataPipeline.py`. Si no se crean utilidades nuevas (logica embebida directamente en scripts de transformacion), no se requiere TDD adicional.

### Paso 5 — Despliegue y validacion

1. Agregar los scripts de plata al pipeline LSDP.
2. Ejecutar el pipeline desde Databricks.
3. Validar:
   - Las vistas materializadas existen en `plata.lab1`.
   - Ninguna columna duplicada en `clientes_saldos_consolidados`.
   - Columnas excluidas (`_rescued_data`, `FechaIngestaDatos`, etc.) no presentes.
   - Campos calculados con valores correctos.
   - Expectativas de calidad visibles en la UI del pipeline.
   - Liquid cluster configurado correctamente.
   - Propiedades Delta activas.

## Validaciones Rapidas (SQL)

```sql
-- Verificar existencia de vistas
SHOW TABLES IN plata.lab1;

-- Verificar columnas de clientes_saldos_consolidados (no debe haber CUSTID duplicado, FechaIngestaDatos, _rescued_data)
DESCRIBE plata.lab1.clientes_saldos_consolidados;

-- Verificar columnas de transacciones_enriquecidas
DESCRIBE plata.lab1.transacciones_enriquecidas;

-- Verificar propiedades Delta
SHOW TBLPROPERTIES plata.lab1.clientes_saldos_consolidados;
SHOW TBLPROPERTIES plata.lab1.transacciones_enriquecidas;

-- Verificar campos calculados en consolidada
SELECT identificador_cliente, clasificacion_riesgo_cliente, categoria_saldo_disponible,
       perfil_actividad_bancaria, huella_identificacion_cliente
FROM plata.lab1.clientes_saldos_consolidados
LIMIT 5;

-- Verificar campos calculados en transaccional
SELECT identificador_transaccion, monto_neto_comisiones, porcentaje_comision_sobre_monto,
       variacion_saldo_transaccion, indicador_impacto_financiero
FROM plata.lab1.transacciones_enriquecidas
LIMIT 5;

-- Verificar Dimension Tipo 1 (un solo registro por cliente)
SELECT identificador_cliente, COUNT(*) AS registros
FROM plata.lab1.clientes_saldos_consolidados
GROUP BY identificador_cliente
HAVING registros > 1;
-- Resultado esperado: 0 filas

-- Verificar LEFT JOIN (clientes sin saldos tienen columnas de saldos en NULL)
SELECT COUNT(*) AS clientes_sin_saldo
FROM plata.lab1.clientes_saldos_consolidados
WHERE saldo_disponible IS NULL;
```
