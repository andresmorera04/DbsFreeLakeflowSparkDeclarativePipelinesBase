# Modelo de Datos: Incremento 5 - LSDP Medalla de Oro - Vistas Materializadas

**Feature**: 005-lsdp-oro-vistas
**Fecha**: 2026-04-05

## Entidades

### 1. Vista Materializada comportamiento_atm_cliente (`oro.regional.comportamiento_atm_cliente`)

**Descripcion**: Vista materializada que agrega las transacciones de la vista materializada de plata `transacciones_enriquecidas` por `identificador_cliente` usando agregacion condicional. Calcula 5 metricas de comportamiento ATM: cantidad de depositos ATM, cantidad de retiros ATM, promedio de montos de depositos ATM, promedio de montos de retiros ATM y total de pagos al saldo. Todas las metricas se basan en la columna `monto_principal` de plata. Los valores nulos se reemplazan con 0 o 0.0 usando `F.coalesce`.
**Cantidad de columnas**: 6 (1 identificador + 5 metricas)
**Origen**: Vista materializada `plata.regional.transacciones_enriquecidas` (lectura cross-catalog con nombre completo)
**Estrategia de Actualizacion**: Refrescamiento gestionado por LSDP (la agregacion condicional sin filtros previos permite optimizacion automatica)
**Liquid Cluster**: `identificador_cliente`
**Expectativas de Calidad**: Ninguna — las expectativas de calidad se validan en plata

#### Columnas

| # | Nombre | Tipo | Origen | Descripcion |
|---|--------|------|--------|-------------|
| 1 | identificador_cliente | LongType | `transacciones_enriquecidas.identificador_cliente` | Identificador unico del cliente. Columna de agrupacion (groupBy). |
| 2 | cantidad_depositos_atm | LongType | `F.count(F.when(tipo_transaccion == "DATM", monto_principal))` | Cantidad total de transacciones de deposito en cajero automatico (DATM) del cliente. Valor por defecto: 0. |
| 3 | cantidad_retiros_atm | LongType | `F.count(F.when(tipo_transaccion == "CATM", monto_principal))` | Cantidad total de transacciones de retiro en cajero automatico (CATM) del cliente. Valor por defecto: 0. |
| 4 | promedio_monto_depositos_atm | DoubleType | `F.avg(F.when(tipo_transaccion == "DATM", monto_principal))` | Promedio de los montos brutos de depositos ATM del cliente. Valor por defecto: 0.0. |
| 5 | promedio_monto_retiros_atm | DoubleType | `F.avg(F.when(tipo_transaccion == "CATM", monto_principal))` | Promedio de los montos brutos de retiros ATM del cliente. Valor por defecto: 0.0. |
| 6 | total_pagos_saldo_cliente | DoubleType | `F.sum(F.when(tipo_transaccion == "PGSL", monto_principal))` | Suma total de los montos brutos de pagos al saldo (PGSL) del cliente. Valor por defecto: 0.0. |

#### Tipos de Transaccion Utilizados (desde Tabla Parametros)

Los tipos de transaccion se leen de la tabla Parametros con la clave `TiposTransaccionesLabBase` (valor: `DATM,CATM,PGSL`). La utilidad `LsdpInsertarTiposTransaccion` inserta esta clave de forma idempotente.

| Codigo AS400 | Variable | Descripcion |
|--------------|----------|-------------|
| DATM | `TIPO_DEPOSITO_ATM` | Deposito en cajero automatico |
| CATM | `TIPO_RETIRO_ATM` | Retiro en cajero automatico |
| PGSL | `TIPO_PAGO_SALDO` | Pago al saldo del cliente |

#### Propiedades Delta

| Propiedad | Valor |
|-----------|-------|
| delta.enableChangeDataFeed | true |
| delta.autoOptimize.autoCompact | true |
| delta.autoOptimize.optimizeWrite | true |
| delta.deletedFileRetentionDuration | interval 30 days |
| delta.logRetentionDuration | interval 60 days |

---

### 2. Vista Materializada resumen_integral_cliente (`oro.regional.resumen_integral_cliente`)

**Descripcion**: Vista materializada que combina datos dimensionales del cliente desde `clientes_saldos_consolidados` (plata) con metricas de comportamiento ATM desde `comportamiento_atm_cliente` (oro) mediante INNER JOIN por `identificador_cliente`. Solo incluye clientes presentes en ambas fuentes. Provee un panorama completo de cada cliente con actividad transaccional: identificacion, datos sociodemograficos, informacion financiera, estado general y metricas de actividad ATM. Dimension Tipo 1 heredada de plata.
**Cantidad de columnas**: 22 (17 columnas dimensionales de plata + 5 metricas de oro)
**Origen**: Vista materializada `plata.regional.clientes_saldos_consolidados` (INNER JOIN) + Vista materializada `oro.regional.comportamiento_atm_cliente`
**Estrategia de Actualizacion**: Refrescamiento gestionado por LSDP
**Liquid Cluster**: `huella_identificacion_cliente`, `identificador_cliente`
**Expectativas de Calidad**: Ninguna — las expectativas se validan en plata

#### Columnas — Identificacion del Cliente (4 columnas)

| # | Nombre | Tipo | Fuente | Descripcion |
|---|--------|------|--------|-------------|
| 1 | huella_identificacion_cliente | StringType | `clientes_saldos_consolidados` | Hash SHA2_256 del identificador de cliente. Campo calculado en plata. |
| 2 | identificador_cliente | LongType | `clientes_saldos_consolidados` | Identificador unico del cliente (CUSTID original). Columna de JOIN. |
| 3 | nombre_cliente | StringType | `clientes_saldos_consolidados` | Nombre del cliente (hebreo, egipcio o ingles). |
| 4 | apellido_cliente | StringType | `clientes_saldos_consolidados` | Apellido del cliente. |

#### Columnas — Datos Sociodemograficos (5 columnas)

| # | Nombre | Tipo | Fuente | Descripcion |
|---|--------|------|--------|-------------|
| 5 | nacionalidad_cliente | StringType | `clientes_saldos_consolidados` | Nacionalidad del cliente. |
| 6 | pais_residencia | StringType | `clientes_saldos_consolidados` | Pais de residencia del cliente. |
| 7 | ciudad_residencia | StringType | `clientes_saldos_consolidados` | Ciudad de residencia del cliente. |
| 8 | ocupacion_cliente | StringType | `clientes_saldos_consolidados` | Ocupacion profesional del cliente. |
| 9 | nivel_educativo | StringType | `clientes_saldos_consolidados` | Nivel educativo alcanzado por el cliente. |

#### Columnas — Datos Financieros y Clasificacion (6 columnas)

| # | Nombre | Tipo | Fuente | Descripcion |
|---|--------|------|--------|-------------|
| 10 | clasificacion_riesgo_cliente | StringType | `clientes_saldos_consolidados` | Clasificacion de riesgo del cliente. Campo calculado en plata (CASE basado en score, ranking prestamos y calificacion crediticia). |
| 11 | categoria_saldo_disponible | StringType | `clientes_saldos_consolidados` | Categoria del saldo disponible del cliente. Campo calculado en plata (CASE basado en saldo disponible, limite credito e ingresos). |
| 12 | perfil_actividad_bancaria | StringType | `clientes_saldos_consolidados` | Perfil de actividad bancaria del cliente. Campo calculado en plata (CASE basado en cantidad transacciones, cantidad cuentas y estado perfil). |
| 13 | limite_credito | DoubleType | `clientes_saldos_consolidados` | Limite de credito asignado al cliente. |
| 14 | saldo_disponible | DoubleType | `clientes_saldos_consolidados` | Saldo disponible actual del cliente. |
| 15 | fecha_apertura_cuenta | DateType | `clientes_saldos_consolidados` | Fecha en que se abrio la cuenta del cliente. |

#### Columnas — Estado General (2 columnas)

| # | Nombre | Tipo | Fuente | Descripcion |
|---|--------|------|--------|-------------|
| 16 | estado_cuenta | StringType | `clientes_saldos_consolidados` | Estado actual de la cuenta (activa, inactiva, etc.). |
| 17 | tipo_cuenta | StringType | `clientes_saldos_consolidados` | Tipo de cuenta del cliente (AHRO, CRTE, PRES, INVR). |

#### Columnas — Metricas ATM (5 columnas)

| # | Nombre | Tipo | Fuente | Descripcion |
|---|--------|------|--------|-------------|
| 18 | cantidad_depositos_atm | LongType | `comportamiento_atm_cliente` | Cantidad de depositos en cajero automatico. |
| 19 | cantidad_retiros_atm | LongType | `comportamiento_atm_cliente` | Cantidad de retiros en cajero automatico. |
| 20 | promedio_monto_depositos_atm | DoubleType | `comportamiento_atm_cliente` | Promedio de montos de depositos ATM. |
| 21 | promedio_monto_retiros_atm | DoubleType | `comportamiento_atm_cliente` | Promedio de montos de retiros ATM. |
| 22 | total_pagos_saldo_cliente | DoubleType | `comportamiento_atm_cliente` | Total de pagos al saldo del cliente. |

#### Propiedades Delta

| Propiedad | Valor |
|-----------|-------|
| delta.enableChangeDataFeed | true |
| delta.autoOptimize.autoCompact | true |
| delta.autoOptimize.optimizeWrite | true |
| delta.deletedFileRetentionDuration | interval 30 days |
| delta.logRetentionDuration | interval 60 days |

---

## Linaje de Datos

```
transacciones_enriquecidas (plata.regional)
    |  groupBy(identificador_cliente) + agregacion condicional
    |  F.count/F.avg/F.sum + F.when(tipo_transaccion == DATM/CATM/PGSL)
    |  F.coalesce para defaults 0/0.0
    v
comportamiento_atm_cliente (oro.regional) — 6 columnas
    |
    +---> INNER JOIN por identificador_cliente
    |
clientes_saldos_consolidados (plata.regional) — 17 columnas seleccionadas
    v
resumen_integral_cliente (oro.regional) — 22 columnas
```

## Dependencias entre Entidades

| Entidad | Depende de | Tipo de Dependencia |
|---------|------------|---------------------|
| `comportamiento_atm_cliente` | `transacciones_enriquecidas` (plata) | Lectura cross-catalog (nombre completo de 3 partes) |
| `resumen_integral_cliente` | `clientes_saldos_consolidados` (plata) | Lectura cross-catalog (nombre completo de 3 partes) |
| `resumen_integral_cliente` | `comportamiento_atm_cliente` (oro) | Lectura intra-schema (nombre completo de 3 partes) |

## Claves de Tabla Parametros Requeridas

| Clave | Valor por defecto | Utilizada por |
|-------|-------------------|---------------|
| catalogoOro | oro | Catalogo destino de las vistas de oro |
| esquemaOro | regional | Esquema destino de las vistas de oro |
| catalogoPlata | plata | Catalogo fuente para leer vistas de plata |
| esquemaPlata | regional | Esquema fuente para leer vistas de plata |
| TiposTransaccionesLabBase | DATM,CATM,PGSL | Tipos de transaccion para metricas ATM (separados por coma) |
