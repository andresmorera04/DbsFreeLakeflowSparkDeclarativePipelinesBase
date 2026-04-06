# Modelo de Datos: Incremento 4 - LSDP Medalla de Plata - Vistas Materializadas

**Feature**: 004-lsdp-plata-vistas
**Fecha**: 2026-04-05

## Entidades

### 1. Vista Materializada clientes_saldos_consolidados (`plata.lab1.clientes_saldos_consolidados`)

**Descripcion**: Vista materializada que consolida el Maestro de Clientes (cmstfl) y los Saldos (blncfl) de bronce como Dimension Tipo 1 (siempre los datos mas recientes por cada cliente). Implementa LEFT JOIN por CUSTID con cmstfl como tabla base. Columnas sin duplicados entre ambas tablas fuente, todas renombradas a espanol snake_case, mas 4 campos calculados.
**Cantidad de columnas**: 173 (70 de cmstfl + 99 de blncfl excluyendo CUSTID/FechaIngestaDatos/_rescued_data + 4 campos calculados)
**Origen**: Streaming tables `bronce.lab1.cmstfl` (72 cols) y `bronce.lab1.blncfl` (102 cols)
**Estrategia de Actualizacion**: Refrescamiento completo (Dimension Tipo 1 con Window + JOIN requiere recalculo)
**Liquid Cluster**: `huella_identificacion_cliente`, `identificador_cliente`
**Expectativas de Calidad**: 5 (`@dp.expect`, modo observacional)

#### Columnas Excluidas (RF-028)

Las siguientes columnas de bronce se descartan y NO aparecen en la vista de plata:

| Columna | Origen | Razon |
|---------|--------|-------|
| FechaIngestaDatos | cmstfl, blncfl | Columna de control de bronce. Usada internamente para Dimension Tipo 1 pero no expuesta. |
| _rescued_data | cmstfl, blncfl | Columna automatica de AutoLoader. Solo relevante en bronce. |
| año | (ambas, si existe) | Columna de particion de bronce. Exclusion solicitada por area de negocio. Manejo gracioso si no existe. |
| mes | (ambas, si existe) | Idem. |
| dia | (ambas, si existe) | Idem. |
| CUSTID (de blncfl) | blncfl | Columna comun resuelta: CUSTID se toma exclusivamente de cmstfl. |

#### Resolucion de Columnas Comunes

| Columna AS400 | Existe en | Se toma de | Razon |
|---------------|-----------|------------|-------|
| CUSTID | cmstfl, blncfl | cmstfl | Identificador de cliente — source of truth del maestro |
| FechaIngestaDatos | cmstfl, blncfl | Descartada | Columna de control de bronce |
| _rescued_data | cmstfl, blncfl | Descartada | Columna de AutoLoader |

#### Mapeo de Columnas — CMSTFL (70 columnas → 70 columnas plata)

**Identificacion Personal / Sociodemografica (28 columnas)**

| # | AS400 | Plata (espanol snake_case) | Tipo | Categoria |
|---|-------|----------------------------|------|-----------|
| 1 | CUSTID | identificador_cliente | LongType | Identificacion |
| 2 | CUSNM | nombre_cliente | StringType | Identificacion |
| 3 | CUSLN | apellido_cliente | StringType | Identificacion |
| 4 | CUSMD | nombre_medio_cliente | StringType | Identificacion |
| 5 | CUSFN | nombre_completo_cliente | StringType | Identificacion |
| 6 | CUSSX | sexo_cliente | StringType | Sociodemografico |
| 7 | CUSTT | tratamiento_cliente | StringType | Sociodemografico |
| 8 | CUSDB | fecha_nacimiento | DateType | Sociodemografico |
| 9 | CUSYR | anio_nacimiento | LongType | Sociodemografico |
| 10 | CUSAG2 | edad_cliente | LongType | Sociodemografico |
| 11 | CUSAD | direccion_calle | StringType | Ubicacion |
| 12 | CUSA2 | direccion_apartamento | StringType | Ubicacion |
| 13 | CUSCT | ciudad_residencia | StringType | Ubicacion |
| 14 | CUSST | estado_provincia | StringType | Ubicacion |
| 15 | CUSZP | codigo_postal | StringType | Ubicacion |
| 16 | CUSCN | pais_residencia | StringType | Ubicacion |
| 17 | CUSNA | nacionalidad_cliente | StringType | Sociodemografico |
| 18 | CUSPH | telefono_principal | StringType | Contacto |
| 19 | CUSMB | telefono_movil | StringType | Contacto |
| 20 | CUSEM | correo_electronico | StringType | Contacto |
| 21 | CUSMS | estado_civil | StringType | Sociodemografico |
| 22 | CUSOC | ocupacion_cliente | StringType | Sociodemografico |
| 23 | CUSED | nivel_educativo | StringType | Sociodemografico |
| 24 | CUSDL | numero_licencia_conducir | StringType | Identificacion |
| 25 | CUSDP | tipo_documento_pasaporte | StringType | Identificacion |
| 26 | CUSDP2 | cantidad_pasaportes | LongType | Identificacion |
| 27 | CUSLG | idioma_preferido | StringType | Sociodemografico |
| 28 | CUSRG | region_geografica | StringType | Ubicacion |

**Comercial / Relacion Bancaria (25 columnas)**

| # | AS400 | Plata (espanol snake_case) | Tipo | Categoria |
|---|-------|----------------------------|------|-----------|
| 29 | CUSTP | tipo_cliente | StringType | Comercial |
| 30 | CUSSG | segmento_cliente | StringType | Comercial |
| 31 | CUSBR | sucursal_principal | StringType | Comercial |
| 32 | CUSMG | gerente_asignado | StringType | Comercial |
| 33 | CUSRF | referencia_interna | StringType | Comercial |
| 34 | CUSRS | fuente_referencia | StringType | Comercial |
| 35 | CUSAG | grupo_afinidad | StringType | Comercial |
| 36 | CUSPC | preferencia_comunicacion | StringType | Comercial |
| 37 | CUSRK | nivel_riesgo | StringType | Comercial |
| 38 | CUSVP | indicador_vip | StringType | Comercial |
| 39 | CUSPF | estado_perfil | StringType | Comercial |
| 40 | CUSKT | estado_kyc | StringType | Comercial |
| 41 | CUSFM | indicador_flags | StringType | Comercial |
| 42 | CUSLC | ultimo_canal | StringType | Comercial |
| 43 | CUSCR | calificacion_crediticia | StringType | Comercial |
| 44 | CUSAC | cuenta_activa | StringType | Comercial |
| 45 | CUSCL | clasificacion_interna | StringType | Comercial |
| 46 | CUSAC2 | cantidad_cuentas | LongType | Comercial |
| 47 | CUSTX | cantidad_transacciones | LongType | Comercial |
| 48 | CUSSC | score_cliente | LongType | Comercial |
| 49 | CUSLR | ranking_prestamos | LongType | Comercial |
| 50 | CUSRC | cantidad_registros | LongType | Comercial |
| 51 | CUSIN | ingresos_cliente | DoubleType | Financiero |
| 52 | CUSBL | saldo_disponible_maestro | DoubleType | Financiero |
| 53 | CUSNT | nota_cliente | StringType | Comercial |

**Fechas Administrativas (17 columnas)**

| # | AS400 | Plata (espanol snake_case) | Tipo |
|---|-------|----------------------------|------|
| 54 | CUSOD | fecha_apertura_relacion | DateType |
| 55 | CUSCD | fecha_cierre_relacion | DateType |
| 56 | CUSLV | fecha_ultima_visita | DateType |
| 57 | CUSUD | fecha_ultima_actualizacion | DateType |
| 58 | CUSKD | fecha_verificacion_kyc | DateType |
| 59 | CUSRD | fecha_renovacion | DateType |
| 60 | CUSXD | fecha_expiracion | DateType |
| 61 | CUSFD | fecha_primer_producto | DateType |
| 62 | CUSLD | fecha_ultimo_producto | DateType |
| 63 | CUSMD2 | fecha_migracion | DateType |
| 64 | CUSAD2 | fecha_activacion | DateType |
| 65 | CUSBD | fecha_bloqueo | DateType |
| 66 | CUSVD | fecha_verificacion | DateType |
| 67 | CUSPD | fecha_promocion | DateType |
| 68 | CUSDD | fecha_desactivacion | DateType |
| 69 | CUSED2 | fecha_educacion_financiera | DateType |
| 70 | CUSND | fecha_notificacion | DateType |

#### Mapeo de Columnas — BLNCFL (99 columnas → 99 columnas plata, excluyendo CUSTID)

**Identificacion de Cuenta (1 columna, excluido CUSTID)**

| # | AS400 | Plata (espanol snake_case) | Tipo | Categoria |
|---|-------|----------------------------|------|-----------|
| 71 | BLSQ | secuencia_saldo | LongType | Identificacion |

**Atributos de Cuenta (29 columnas)**

| # | AS400 | Plata (espanol snake_case) | Tipo | Categoria |
|---|-------|----------------------------|------|-----------|
| 72 | BLACT | tipo_cuenta | StringType | Cuenta |
| 73 | BLACN | numero_cuenta | StringType | Cuenta |
| 74 | BLCUR | moneda_cuenta | StringType | Cuenta |
| 75 | BLST | estado_cuenta | StringType | Cuenta |
| 76 | BLBR | sucursal_cuenta | StringType | Cuenta |
| 77 | BLPR | producto_cuenta | StringType | Cuenta |
| 78 | BLSP | subproducto_cuenta | StringType | Cuenta |
| 79 | BLNM | nombre_cuenta | StringType | Cuenta |
| 80 | BLCL | clase_cuenta | StringType | Cuenta |
| 81 | BLRK | riesgo_cuenta | StringType | Cuenta |
| 82 | BLTP | tipo_producto_cuenta | StringType | Cuenta |
| 83 | BLMG | gerente_cuenta | StringType | Cuenta |
| 84 | BLRF | referencia_cuenta | StringType | Cuenta |
| 85 | BLCC | centro_costos_cuenta | StringType | Cuenta |
| 86 | BLAG | grupo_afinidad_cuenta | StringType | Cuenta |
| 87 | BLPL | plan_cuenta | StringType | Cuenta |
| 88 | BLRG | region_cuenta | StringType | Cuenta |
| 89 | BLSF | sufijo_cuenta | StringType | Cuenta |
| 90 | BLNT | nota_cuenta | StringType | Cuenta |
| 91 | BLLC | ultimo_canal_cuenta | StringType | Cuenta |
| 92 | BLPF | perfil_cuenta | StringType | Cuenta |
| 93 | BLAU | autorizado_cuenta | StringType | Cuenta |
| 94 | BLTX | texto_cuenta | StringType | Cuenta |
| 95 | BLGR | grupo_cuenta | StringType | Cuenta |
| 96 | BLEM | email_cuenta | StringType | Cuenta |
| 97 | BLFR | frecuencia_cuenta | StringType | Cuenta |
| 98 | BLKY | clave_cuenta | StringType | Cuenta |
| 99 | BLVP | vip_cuenta | StringType | Cuenta |
| 100 | BLFC | factor_cuenta | StringType | Cuenta |

**Saldos y Montos (34 columnas)**

| # | AS400 | Plata (espanol snake_case) | Tipo | Categoria |
|---|-------|----------------------------|------|-----------|
| 101 | BLAV | saldo_disponible | DoubleType | Saldo |
| 102 | BLTB | saldo_total | DoubleType | Saldo |
| 103 | BLRV | saldo_reservado | DoubleType | Saldo |
| 104 | BLBK | saldo_bloqueado | DoubleType | Saldo |
| 105 | BLCR | limite_credito | DoubleType | Saldo |
| 106 | BLCN | credito_utilizado | DoubleType | Saldo |
| 107 | BLCD | credito_disponible | DoubleType | Saldo |
| 108 | BLOV | valor_sobregiro | DoubleType | Saldo |
| 109 | BLOL | limite_sobregiro | DoubleType | Saldo |
| 110 | BLPD | depositos_pendientes | DoubleType | Saldo |
| 111 | BLPC | cargos_pendientes | DoubleType | Saldo |
| 112 | BLPA | ajustes_pendientes | DoubleType | Saldo |
| 113 | BLDI | depositos_ingreso | DoubleType | Movimiento |
| 114 | BLWI | retenciones_cuenta | DoubleType | Movimiento |
| 115 | BLTI | transferencias_ingreso | DoubleType | Movimiento |
| 116 | BLTC | cargos_transferencia | DoubleType | Movimiento |
| 117 | BLCA | comisiones_anuales | DoubleType | Comision |
| 118 | BLIM | intereses_mensuales | DoubleType | Interes |
| 119 | BLRF2 | reembolsos_cuenta | DoubleType | Movimiento |
| 120 | BLPN | penalidades_cuenta | DoubleType | Comision |
| 121 | BLBN | bonificaciones_cuenta | DoubleType | Movimiento |
| 122 | BLAP | ajustes_positivos | DoubleType | Movimiento |
| 123 | BLAM | ajustes_miscelaneos | DoubleType | Movimiento |
| 124 | BLAY | ajustes_anuales | DoubleType | Movimiento |
| 125 | BLHI | marca_alta_saldo | DoubleType | Estadistica |
| 126 | BLLO | marca_baja_saldo | DoubleType | Estadistica |
| 127 | BLVR | varianza_saldo | DoubleType | Estadistica |
| 128 | BLRT | ratio_cuenta | DoubleType | Estadistica |
| 129 | BLCP | porcentaje_aporte | DoubleType | Estadistica |
| 130 | BLCI | ingresos_aporte | DoubleType | Estadistica |
| 131 | BLMN | saldo_minimo | DoubleType | Saldo |
| 132 | BLMX | saldo_maximo | DoubleType | Saldo |
| 133 | BLIR | tasa_interes | DoubleType | Interes |
| 134 | BLPM | multiplicador_penalidad | DoubleType | Comision |

**Fechas de Cuenta (35 columnas)**

| # | AS400 | Plata (espanol snake_case) | Tipo |
|---|-------|----------------------------|------|
| 135 | BLOD | fecha_apertura_cuenta | DateType |
| 136 | BLXD | fecha_expiracion_cuenta | DateType |
| 137 | BLUD | fecha_actualizacion_cuenta | DateType |
| 138 | BLLD | fecha_ultimo_movimiento | DateType |
| 139 | BLSD | fecha_estado_cuenta | DateType |
| 140 | BLPD2 | fecha_penalidad | DateType |
| 141 | BLRD | fecha_renovacion_cuenta | DateType |
| 142 | BLMD | fecha_maduracion | DateType |
| 143 | BLCD2 | fecha_cierre_cuenta | DateType |
| 144 | BLBD | fecha_bloqueo_cuenta | DateType |
| 145 | BLFD | fecha_fondeo | DateType |
| 146 | BLGD | fecha_gracia | DateType |
| 147 | BLHD | fecha_historica | DateType |
| 148 | BLID | fecha_interes | DateType |
| 149 | BLJD | fecha_ajuste | DateType |
| 150 | BLKD | fecha_kyc_cuenta | DateType |
| 151 | BLND | fecha_notificacion_cuenta | DateType |
| 152 | BLTD | fecha_transferencia | DateType |
| 153 | BLVD | fecha_verificacion_cuenta | DateType |
| 154 | BLWD | fecha_retiro | DateType |
| 155 | BLYD | fecha_rendimiento | DateType |
| 156 | BLZD | fecha_cierre_periodo | DateType |
| 157 | BLED | fecha_evaluacion | DateType |
| 158 | BLAD2 | fecha_activacion_cuenta | DateType |
| 159 | BLDD | fecha_desactivacion_cuenta | DateType |
| 160 | BLFP | fecha_primer_pago | DateType |
| 161 | BLLP | fecha_ultimo_pago | DateType |
| 162 | BLMP | fecha_mora_pago | DateType |
| 163 | BLNP | fecha_proximo_pago | DateType |
| 164 | BLOP | fecha_origen_pago | DateType |
| 165 | BLPP | fecha_programacion_pago | DateType |
| 166 | BLQP | fecha_quiebre_pago | DateType |
| 167 | BLRP | fecha_regularizacion_pago | DateType |
| 168 | BLSP2 | fecha_suspension_pago | DateType |
| 169 | BLTP2 | fecha_tercero_pago | DateType |

#### Campos Calculados (4 columnas)

| # | Campo Plata | Tipo | Columnas Fuente (AS400) | Logica |
|---|-------------|------|------------------------|--------|
| 170 | clasificacion_riesgo_cliente | StringType | CUSRK (nivel_riesgo), CUSCR (calificacion_crediticia), CUSSC (score_cliente) | `CASE WHEN score_cliente >= 750 AND nivel_riesgo IN ('L','LOW') AND calificacion_crediticia IN ('A','AA','AAA') THEN 'RIESGO_BAJO' WHEN score_cliente >= 500 AND nivel_riesgo IN ('M','MED') THEN 'RIESGO_MEDIO' WHEN score_cliente < 500 OR nivel_riesgo IN ('H','HIGH') THEN 'RIESGO_ALTO' ELSE 'SIN_CLASIFICAR'` |
| 171 | categoria_saldo_disponible | StringType | BLAV (saldo_disponible), BLCR (limite_credito), BLTB (saldo_total) | `CASE WHEN saldo_disponible >= 100000 AND limite_credito >= 50000 AND saldo_total >= 150000 THEN 'PREMIUM' WHEN saldo_disponible >= 25000 AND limite_credito >= 10000 THEN 'ESTANDAR' WHEN saldo_disponible > 0 THEN 'BASICO' ELSE 'SIN_SALDO'` |
| 172 | perfil_actividad_bancaria | StringType | CUSTX (cantidad_transacciones), CUSAC2 (cantidad_cuentas), CUSLR (ranking_prestamos) | `CASE WHEN cantidad_transacciones >= 100 AND cantidad_cuentas >= 3 AND ranking_prestamos >= 5 THEN 'MUY_ACTIVO' WHEN cantidad_transacciones >= 30 AND cantidad_cuentas >= 2 THEN 'ACTIVO' WHEN cantidad_transacciones >= 1 THEN 'MODERADO' ELSE 'INACTIVO'` |
| 173 | huella_identificacion_cliente | StringType | CUSTID (identificador_cliente) | `SHA2(CAST(identificador_cliente AS STRING), 256)` — hash hexadecimal de 64 caracteres |

#### Expectativas de Calidad de Datos (5 reglas, modo observacional `@dp.expect`)

| # | Nombre Expectativa | Condicion SQL | Columna Plata | Columna AS400 |
|---|-------------------|---------------|---------------|---------------|
| 1 | limite_credito_positivo | `limite_credito > 0` | limite_credito | BLCR |
| 2 | identificador_cliente_no_nulo | `identificador_cliente IS NOT NULL` | identificador_cliente | CUSTID |
| 3 | limite_credito_no_nulo | `limite_credito IS NOT NULL` | limite_credito | BLCR |
| 4 | fecha_apertura_cuenta_valida | `fecha_apertura_cuenta > '2020-12-31'` | fecha_apertura_cuenta | BLOD |
| 5 | fecha_nacimiento_valida | `fecha_nacimiento < '2009-01-01'` | fecha_nacimiento | CUSDB |

#### Propiedades Delta (via `table_properties` del decorador `@dp.materialized_view`)

| Propiedad | Valor |
|-----------|-------|
| delta.enableChangeDataFeed | true |
| delta.autoOptimize.autoCompact | true |
| delta.autoOptimize.optimizeWrite | true |
| delta.deletedFileRetentionDuration | interval 30 days |
| delta.logRetentionDuration | interval 60 days |

#### Reglas de Transformacion

- **Dimension Tipo 1**: Ambas tablas de bronce (cmstfl y blncfl) se filtran individualmente para obtener solo el registro mas reciente por CUSTID (Window + ROW_NUMBER por FechaIngestaDatos DESC, desempate por CUSTID DESC) ANTES del JOIN.
- **LEFT JOIN**: cmstfl (base) LEFT JOIN blncfl ON identificador_cliente. Clientes sin saldos se incluyen con columnas de saldos en NULL.
- **Deduplicacion**: CUSTID se toma de cmstfl. Columnas comunes (FechaIngestaDatos, _rescued_data) descartadas.
- **Exclusion de columnas**: `_rescued_data`, `año`, `mes`, `dia`, `FechaIngestaDatos` de ambas tablas. Manejo gracioso si `año/mes/dia` no existen.
- **Renombrado**: Todas las columnas de AS400 a espanol snake_case via diccionario de mapeo.
- **Campos calculados**: Se agregan despues del JOIN y renombrado, usando columnas ya renombradas.
- **Liquid Cluster**: `huella_identificacion_cliente` e `identificador_cliente` en posiciones 1 y 2 (reordenadas via `LsdpReordenarColumnasLiquidCluster`).

---

### 2. Vista Materializada transacciones_enriquecidas (`plata.lab1.transacciones_enriquecidas`)

**Descripcion**: Vista materializada a partir de la streaming table transaccional (trxpfl) de bronce. Sin filtros para maximizar carga incremental automatica de LSDP. Incluye 4 campos calculados numericos. Todas las columnas renombradas a espanol snake_case.
**Cantidad de columnas**: 64 (60 originales del parquet TRXPFL - columnas excluidas + 4 campos calculados)
**Origen**: Streaming table `bronce.lab1.trxpfl` (62 cols)
**Estrategia de Actualizacion**: Carga incremental automatica (sin filtros, sin funciones no deterministas)
**Liquid Cluster**: `fecha_transaccion`, `identificador_cliente`, `tipo_transaccion`
**Expectativas de Calidad**: 4 (`@dp.expect`, modo observacional)

#### Columnas Excluidas (RF-028)

| Columna | Razon |
|---------|-------|
| FechaIngestaDatos | Columna de control de bronce |
| _rescued_data | Columna automatica de AutoLoader |
| año | Columna de particion (si existe). Manejo gracioso. |
| mes | Idem. |
| dia | Idem. |

#### Mapeo de Columnas — TRXPFL (60 columnas → 60 columnas plata)

**Identificacion Transaccional (3 columnas)**

| # | AS400 | Plata (espanol snake_case) | Tipo | Categoria |
|---|-------|----------------------------|------|-----------|
| 1 | TRXID | identificador_transaccion | StringType | Identificacion |
| 2 | CUSTID | identificador_cliente | LongType | Identificacion |
| 3 | TRXSQ | secuencia_transaccion | LongType | Identificacion |

**Atributos de Transaccion (6 columnas)**

| # | AS400 | Plata (espanol snake_case) | Tipo | Categoria |
|---|-------|----------------------------|------|-----------|
| 4 | TRXTYP | tipo_transaccion | StringType | Atributo |
| 5 | TRXCUR | moneda_transaccion | StringType | Atributo |
| 6 | TRXST | estado_transaccion | StringType | Atributo |
| 7 | TRXCH | canal_transaccion | StringType | Atributo |
| 8 | TRXDSC | descripcion_transaccion | StringType | Atributo |
| 9 | TRXREF | referencia_externa | StringType | Atributo |

**Montos y Valores (30 columnas)**

| # | AS400 | Plata (espanol snake_case) | Tipo | Categoria |
|---|-------|----------------------------|------|-----------|
| 10 | TRXAMT | monto_principal | DoubleType | Monto |
| 11 | TRXCM | comision_transaccion | DoubleType | Comision |
| 12 | TRXBA | saldo_posterior | DoubleType | Saldo |
| 13 | TRXBP | saldo_anterior | DoubleType | Saldo |
| 14 | TRXTC | cargo_fiscal | DoubleType | Cargo |
| 15 | TRXAL | monto_local | DoubleType | Monto |
| 16 | TRXPN | monto_pago | DoubleType | Monto |
| 17 | TRXBF | beneficio_transaccion | DoubleType | Monto |
| 18 | TRXRL | perdida_tasa | DoubleType | Monto |
| 19 | TRXMX | monto_maximo | DoubleType | Estadistica |
| 20 | TRXMN | monto_minimo | DoubleType | Estadistica |
| 21 | TRXAV | monto_promedio | DoubleType | Estadistica |
| 22 | TRXDV | desviacion_monto | DoubleType | Estadistica |
| 23 | TRXRK | riesgo_transaccion | DoubleType | Riesgo |
| 24 | TRXFR | riesgo_fraude | DoubleType | Riesgo |
| 25 | TRXLM | limite_transaccion | DoubleType | Limite |
| 26 | TRXLP | porcentaje_limite | DoubleType | Limite |
| 27 | TRXCP | cargo_plataforma | DoubleType | Cargo |
| 28 | TRXCI | cargo_institucion | DoubleType | Cargo |
| 29 | TRXCF | cargo_extranjero | DoubleType | Cargo |
| 30 | TRXCV | cargo_varianza | DoubleType | Cargo |
| 31 | TRXSB | subtotal_transaccion | DoubleType | Monto |
| 32 | TRXTL | total_transaccion | DoubleType | Monto |
| 33 | TRXRS | residuo_transaccion | DoubleType | Monto |
| 34 | TRXIM | margen_interes | DoubleType | Interes |
| 35 | TRXNT | monto_neto | DoubleType | Monto |
| 36 | TRXAO | monto_original | DoubleType | Monto |
| 37 | TRXIN | monto_inversion | DoubleType | Monto |
| 38 | TRXDS | descuento_transaccion | DoubleType | Monto |
| 39 | TRXPT | monto_principal_prestamo | DoubleType | Monto |

**Fechas (19 columnas)**

| # | AS400 | Plata (espanol snake_case) | Tipo |
|---|-------|----------------------------|------|
| 40 | TRXDT | fecha_transaccion | DateType |
| 41 | TRXVD | fecha_valor | DateType |
| 42 | TRXPD | fecha_procesamiento | DateType |
| 43 | TRXSD | fecha_liquidacion | DateType |
| 44 | TRXCD | fecha_compensacion | DateType |
| 45 | TRXED | fecha_efectiva | DateType |
| 46 | TRXRD | fecha_reverso | DateType |
| 47 | TRXAD | fecha_autorizacion | DateType |
| 48 | TRXND | fecha_notificacion_trx | DateType |
| 49 | TRXXD | fecha_expiracion_trx | DateType |
| 50 | TRXFD | fecha_fondeo_trx | DateType |
| 51 | TRXGD | fecha_gracia_trx | DateType |
| 52 | TRXHD | fecha_historica_trx | DateType |
| 53 | TRXBD | fecha_bloqueo_trx | DateType |
| 54 | TRXMD | fecha_maduracion_trx | DateType |
| 55 | TRXLD | fecha_limite_trx | DateType |
| 56 | TRXUD | fecha_actualizacion_trx | DateType |
| 57 | TRXOD | fecha_origen_trx | DateType |
| 58 | TRXKD | fecha_kyc_trx | DateType |

**Timestamps (2 columnas)**

| # | AS400 | Plata (espanol snake_case) | Tipo |
|---|-------|----------------------------|------|
| 59 | TRXTS | timestamp_transaccion | TimestampType |
| 60 | TRXUS | timestamp_actualizacion | TimestampType |

#### Campos Calculados (4 columnas numericas)

| # | Campo Plata | Tipo | Columnas Fuente (AS400) | Logica |
|---|-------------|------|------------------------|--------|
| 61 | monto_neto_comisiones | DoubleType | TRXAMT (monto_principal), TRXCM (comision_transaccion) | `COALESCE(monto_principal, 0.0) - COALESCE(comision_transaccion, 0.0)` |
| 62 | porcentaje_comision_sobre_monto | DoubleType | TRXCM (comision_transaccion), TRXAMT (monto_principal) | `CASE WHEN monto_principal != 0 AND monto_principal IS NOT NULL THEN (COALESCE(comision_transaccion, 0.0) / monto_principal) * 100.0 ELSE 0.0` |
| 63 | variacion_saldo_transaccion | DoubleType | TRXBA (saldo_posterior), TRXBP (saldo_anterior) | `COALESCE(saldo_posterior, 0.0) - COALESCE(saldo_anterior, 0.0)` |
| 64 | indicador_impacto_financiero | DoubleType | TRXAMT (monto_principal), TRXRK (riesgo_transaccion) | `COALESCE(monto_principal, 0.0) * COALESCE(riesgo_transaccion, 0.0)` |

#### Expectativas de Calidad de Datos (4 reglas, modo observacional `@dp.expect`)

| # | Nombre Expectativa | Condicion SQL | Columna Plata | Columna AS400 |
|---|-------------------|---------------|---------------|---------------|
| 1 | moneda_transaccion_no_nula | `moneda_transaccion IS NOT NULL` | moneda_transaccion | TRXCUR |
| 2 | monto_neto_no_nulo | `monto_neto IS NOT NULL` | monto_neto | TRXNT |
| 3 | monto_neto_positivo | `monto_neto > 0` | monto_neto | TRXNT |
| 4 | identificador_cliente_no_nulo | `identificador_cliente IS NOT NULL` | identificador_cliente | CUSTID |

#### Propiedades Delta

Identicas a `clientes_saldos_consolidados` (5 propiedades Delta en tabla anterior).

#### Reglas de Transformacion

- **Sin filtros**: La lectura de `trxpfl` se realiza sin `.filter()`, `.where()` ni condiciones. LSDP optimiza automaticamente con carga incremental.
- **Sin Dimension Tipo 1**: A diferencia de la vista consolidada, la transaccional NO aplica deduplicacion por CUSTID. Se conservan todas las transacciones.
- **Exclusion de columnas**: `_rescued_data`, `año`, `mes`, `dia`, `FechaIngestaDatos`. Manejo gracioso si no existen.
- **Renombrado**: Todas las columnas de AS400 a espanol snake_case via diccionario de mapeo.
- **Campos calculados**: Se agregan despues del renombrado, operaciones aritmeticas con proteccion de nulos y division por cero.
- **Liquid Cluster**: `fecha_transaccion`, `identificador_cliente`, `tipo_transaccion` (reordenadas via `LsdpReordenarColumnasLiquidCluster`).

---

## Diagrama de Linaje

```text
[Parquets AS400]
     |
     v
[bronce.lab1.cmstfl]  (72 cols, streaming table)
[bronce.lab1.blncfl]  (102 cols, streaming table)
[bronce.lab1.trxpfl]  (62 cols, streaming table)
     |                          |
     v                          v
[Dimension Tipo 1]         [Sin filtros]
[LEFT JOIN por CUSTID]     [Lectura directa]
[Dedup columnas]           [Exclusion columnas]
[Renombrado snake_case]    [Renombrado snake_case]
[4 campos calculados]      [4 campos calculados]
[5 @dp.expect]             [4 @dp.expect]
     |                          |
     v                          v
[plata.lab1.              [plata.lab1.
 clientes_saldos_consolidados] transacciones_enriquecidas]
 (173 cols, materialized view) (64 cols, materialized view)
```

## Transiciones de Estado

No aplica para este incremento — las vistas materializadas no tienen transiciones de estado. Se recalculan declarativamente por LSDP.
