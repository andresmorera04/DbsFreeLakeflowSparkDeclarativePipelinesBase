# Guia Rapida: Incremento 5 - LSDP Medalla de Oro - Vistas Materializadas

**Feature**: 005-lsdp-oro-vistas
**Fecha**: 2026-04-05

## Prerequisitos

1. **Incrementos anteriores completados**:
   - Inc. 1: Tabla Parametros creada en Unity Catalog con todas las claves, incluyendo `catalogoOro`, `esquemaOro`, `catalogoPlata`, `esquemaPlata`.
   - Inc. 2: Parquets AS400 generados (CMSTFL, TRXPFL, BLNCFL).
   - Inc. 3: Pipeline LSDP con medalla de bronce funcional. Streaming tables `bronce.lab1.cmstfl`, `bronce.lab1.trxpfl`, `bronce.lab1.blncfl` pobladas.
   - Inc. 4: Vistas materializadas de plata funcionales. `plata.lab1.clientes_saldos_consolidados` y `plata.lab1.transacciones_enriquecidas` pobladas.

2. **Entorno Databricks**:
   - Databricks Free Edition con Serverless Compute disponible.
   - Catalogos `oro` y esquema `lab1` creados por `conf/NbConfiguracionInicial.py`.
   - Extension Databricks para VS Code configurada y conectada al workspace.

3. **Tabla Parametros con claves requeridas**:

| Clave | Valor esperado |
|-------|---------------|
| catalogoOro | oro |
| esquemaOro | lab1 |
| catalogoPlata | plata |
| esquemaPlata | lab1 |
| TiposTransaccionesLabBase | DATM,CATM,PGSL |

## Estructura de Archivos Nuevos

```text
src/LSDP_Laboratorio_Basico/transformations/
└── LsdpOroClientes.py     # Vistas materializadas comportamiento_atm_cliente y resumen_integral_cliente

src/LSDP_Laboratorio_Basico/utilities/
└── LsdpInsertarTiposTransaccion.py   # Utilidad idempotente para insertar clave TiposTransaccionesLabBase en Parametros
```

## Configuracion del Pipeline LSDP

El script de oro se agrega al mismo pipeline LSDP existente de los Incrementos 3 y 4. El archivo `LsdpOroClientes.py` debe registrarse como source file adicional en la configuracion del pipeline.

**Parametros del pipeline** (ya existentes desde Inc. 3):

| Parametro | Valor (ejemplo) |
|-----------|----------------|
| catalogoParametro | control |
| esquemaParametro | lab1 |
| tablaParametros | Parametros |

**Catalogo y esquema destino de oro**: El pipeline LSDP se configura con el catalogo por defecto `bronce` y esquema `lab1`. Las vistas materializadas de oro se crean en `oro.lab1` usando los parametros `catalog` y `schema` del decorador `@dp.materialized_view`.

## Flujo de Desarrollo

### Paso 1 — Verificar prerequisitos de plata

Ejecutar el pipeline LSDP existente y confirmar que las 2 vistas materializadas de plata contienen datos:
- `plata.lab1.clientes_saldos_consolidados`
- `plata.lab1.transacciones_enriquecidas`

### Paso 2 — Implementar LsdpInsertarTiposTransaccion.py

1. Crear el archivo en `src/LSDP_Laboratorio_Basico/utilities/LsdpInsertarTiposTransaccion.py`.
2. Implementar funcion `insertar_tipos_transaccion(spark, catalogo, esquema, tabla_parametros)`:
   - Consulta si la clave `TiposTransaccionesLabBase` ya existe.
   - Si NO existe, ejecuta INSERT con valor `DATM,CATM,PGSL`.
   - Si YA existe, no opera (idempotente).
   - Imprime resultado para observabilidad.
3. Crear TDD para la utilidad.

### Paso 3 — Implementar LsdpOroClientes.py

1. Crear el archivo en `src/LSDP_Laboratorio_Basico/transformations/LsdpOroClientes.py`.
2. Implementar inicializacion del modulo (patron Closure):
   - Leer parametros del pipeline via `spark.conf.get()`.
   - Leer tabla Parametros via `obtener_parametros()`.
   - Extraer `catalogoOro`, `esquemaOro`, `catalogoPlata`, `esquemaPlata`.
   - Leer clave `TiposTransaccionesLabBase` de la tabla Parametros y parsear con `.split(",")`.
   - Definir diccionario de propiedades Delta.
   - Imprimir todos los parametros para observabilidad.
3. Implementar `comportamiento_atm_cliente`:
   - Leer `transacciones_enriquecidas` con nombre completo de 3 partes.
   - `groupBy("identificador_cliente")` con 5 agregaciones condicionales.
   - `F.coalesce` para manejar nulos.
4. Implementar `resumen_integral_cliente`:
   - Leer `clientes_saldos_consolidados` con nombre completo de 3 partes.
   - Leer `comportamiento_atm_cliente` con nombre completo de 3 partes.
   - INNER JOIN por `identificador_cliente`.
   - Seleccionar 22 columnas.

### Paso 4 — Invocar utilidad desde notebook LSDP

Desde el notebook que orquesta el pipeline LSDP, invocar `insertar_tipos_transaccion()` antes de iniciar el pipeline para asegurar que la clave `TiposTransaccionesLabBase` existe en la tabla Parametros.

### Paso 5 — Registrar en el pipeline LSDP

Agregar `LsdpOroClientes.py` como source file en la configuracion del pipeline LSDP existente (en la UI de Databricks o via API).

### Paso 6 — Ejecutar pipeline completo

Ejecutar el pipeline LSDP completo (bronce + plata + oro) y verificar:
- Las 3 streaming tables de bronce se actualizan correctamente.
- Las 2 vistas materializadas de plata se actualizan correctamente.
- Las 2 vistas materializadas de oro se crean en `oro.lab1`.
- `comportamiento_atm_cliente` tiene 6 columnas con metricas correctas.
- `resumen_integral_cliente` tiene 22 columnas con INNER JOIN correcto.
- Los logs muestran todos los prints de observabilidad.

### Paso 7 — Validar no-regresion TDD

Ejecutar el archivo TDD existente (`NbTddBroncePipeline.py`) desde la extension Databricks para VS Code y verificar que todas las pruebas siguen pasando sin errores.

## Verificacion Rapida

Despues de la ejecucion exitosa del pipeline, ejecutar las siguientes consultas SQL en Databricks para verificar:

```sql
-- Verificar comportamiento_atm_cliente
SELECT COUNT(*) as total_clientes FROM oro.lab1.comportamiento_atm_cliente;

-- Verificar metricas no nulas
SELECT * FROM oro.lab1.comportamiento_atm_cliente
WHERE cantidad_depositos_atm IS NULL OR cantidad_retiros_atm IS NULL
LIMIT 5;
-- Esperado: 0 filas (todas las metricas deben ser 0, no NULL)

-- Verificar resumen_integral_cliente (solo clientes con actividad transaccional)
SELECT COUNT(*) as total_clientes FROM oro.lab1.resumen_integral_cliente;

-- Verificar 22 columnas
DESCRIBE oro.lab1.resumen_integral_cliente;

-- Verificar INNER JOIN correcto (todos los clientes deben tener contraparte en comportamiento)
SELECT r.identificador_cliente
FROM oro.lab1.resumen_integral_cliente r
LEFT JOIN oro.lab1.comportamiento_atm_cliente c
  ON r.identificador_cliente = c.identificador_cliente
WHERE c.identificador_cliente IS NULL
LIMIT 5;
-- Esperado: 0 filas (INNER JOIN garantiza contraparte)
```
