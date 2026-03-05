# Análisis de Datos Online Retail con PySpark

## Resultados del Análisis

### 1. Número Total de Facturas
- **25,900** facturas únicas en el dataset

### 2. Número de Clientes Únicos
- **4,373** clientes diferentes

### 3. Ingreso Total
- **$9,747,747.93** (Quantity × UnitPrice)

### 4. Producto Más Vendido
- **WORLD WAR 2 GLIDERS ASSTD DESIGNS** con 53,847 unidades

### 5. Cliente con Mayor Volumen de Compra
- **Cliente ID 14646** con $279,489.02 en compras

### 6. Top 5 Países que Más Compran (fuera de Reino Unido)
| País | Ventas Totales |
|------|----------------|
| Netherlands | $284,661.54 |
| EIRE | $263,276.82 |
| Germany | $221,698.21 |
| France | $197,403.90 |
| Australia | $137,077.27 |

### 7. Ticket Promedio por Factura
- **$376.36**

### 8. Productos por Factura
- Mínimo: 1
- Máximo: 1,114
- Promedio: 20.92

### 9. Mes con Más Ventas
- **Noviembre 2011** con $1,461,756.25

### 10. Porcentaje de Facturas con Devoluciones
- **19.97%** de las facturas tienen valores negativos en Quantity

---

## Operaciones PySpark Aplicadas

1. **Lectura de datos**: `spark.createDataFrame()` desde pandas (lectura de Excel)
2. **Selección de columnas**: `select()`
3. **Filtrado de datos**: `filter()` y `where()`
4. **Ordenamiento**: `orderBy()`
5. **Agregaciones**: `sum()`, `avg()`, `min()`, `max()`, `count()`
6. **Agrupación**: `groupBy()` + `agg()`
7. **Creación de columnas derivadas**: `withColumn()`
8. **Uniones (joins)**: `join()` entre DataFrames
9. **Funciones de ventana**: `rank()`, `row_number()` para ranking de clientes
10. **Exportación**: `write.csv()`

---

## Conclusiones

El dataset de Online Retail contiene más de 540,000 transacciones de una tienda online internacional. Las principales conclusiones son:

1. **Dominio del mercado local**: Reino Unido representa la gran mayoría de las ventas
2. **Alta tasa de devoluciones**: Casi el 20% de las facturas incluyen devoluciones
3. **Concentración de clientes**: Pocos clientes generan la mayor parte de los ingresos
4. **Temporalidad**: Noviembre es el mes峰值 de ventas, probablemente por la temporada navideña
5. **Mercados internacionales**: Los países europeos ( Netherlands, EIRE, Germany, France) son los principales compradores fuera de UK

---

## Repositorio GitHub
https://github.com/FireSouls25/pyspark-taller1.git
