from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, avg, min as spark_min, max as spark_max, count,
    when, month, year, desc, asc, upper, lower, trim, 
    rank, row_number, expr, round as spark_round, lit, to_date, substring
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType
import pandas as pd

spark = SparkSession.builder \
    .appName("OnlineRetailAnalysis") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("FATAL")

pandas_df = pd.read_excel("online+retail/Online Retail.xlsx")
pandas_df['CustomerID'] = pandas_df['CustomerID'].fillna(0)
df = spark.createDataFrame(pandas_df)

df = df.withColumn("Quantity", col("Quantity").cast(IntegerType()))
df = df.withColumn("UnitPrice", col("UnitPrice").cast(DoubleType()))

df_with_revenue = df.withColumn("Revenue", spark_round(col("Quantity") * col("UnitPrice"), 2))
df_with_revenue_clean = df_with_revenue.filter(col("CustomerID") != 0)

resultados = {}

print("=" * 60)
print("1. NÚMERO TOTAL DE REGISTROS")
print("=" * 60)
total_registros = df.count()
print(f"Total de registros: {total_registros}")
resultados["01_total_registros"] = pd.DataFrame({"TotalRegistros": [total_registros]})

print("\n" + "=" * 60)
print("2. NÚMERO DE CLIENTES ÚNICOS")
print("=" * 60)
unique_customers = df.select("CustomerID").distinct().count()
print(f"Total de clientes únicos: {unique_customers}")
resultados["02_clientes_unicos"] = pd.DataFrame({"TotalClientes": [unique_customers]})

print("\n" + "=" * 60)
print("3. INGRESO TOTAL")
print("=" * 60)
total_revenue = df_with_revenue.select(sum("Revenue")).collect()[0][0]
print(f"Ingreso total: ${total_revenue:,.2f}")
resultados["03_ingreso_total"] = pd.DataFrame({"IngresoTotal": [round(float(total_revenue), 2)]})

print("\n" + "=" * 60)
print("4. PRODUCTO MÁS VENDIDO EN CANTIDAD")
print("=" * 60)
top_product = df.groupBy("Description").agg(sum("Quantity").alias("TotalQuantity")) \
    .orderBy(desc("TotalQuantity")).first()
print(f"Producto más vendido: {top_product['Description']}")
print(f"Cantidad total: {top_product['TotalQuantity']}")
resultados["04_producto_mas_vendido"] = pd.DataFrame({
    "Producto": [top_product['Description']],
    "CantidadTotal": [top_product['TotalQuantity']]
})

print("\n" + "=" * 60)
print("5. CLIENTE CON MAYOR VOLUMEN DE COMPRA")
print("=" * 60)
top_customer = df_with_revenue_clean.groupBy("CustomerID").agg(sum("Revenue").alias("TotalSpent")) \
    .orderBy(desc("TotalSpent")).first()
print(f"Cliente con mayor volumen: {int(top_customer['CustomerID'])}")
print(f"Monto total: ${top_customer['TotalSpent']:,.2f}")
resultados["05_top_cliente"] = pd.DataFrame({
    "CustomerID": [int(top_customer['CustomerID'])],
    "TotalSpent": [round(float(top_customer['TotalSpent']), 2)]
})

print("\n" + "=" * 60)
print("6. 5 PAÍSES QUE MÁS COMPRAN FUERA DE REINO UNIDO")
print("=" * 60)
non_uk_sales = df_with_revenue.filter(col("Country") != "United Kingdom") \
    .groupBy("Country").agg(spark_round(sum("Revenue"), 2).alias("TotalSales")) \
    .orderBy(desc("TotalSales")).limit(5).toPandas()
print(non_uk_sales.to_string(index=False))
resultados["06_top_paises_sin_uk"] = non_uk_sales

print("\n" + "=" * 60)
print("7. PROMEDIO DE GASTO POR CLIENTE")
print("=" * 60)
avg_customer_spend = df_with_revenue_clean.groupBy("CustomerID").agg(sum("Revenue").alias("TotalSpent"))
avg_spend = avg_customer_spend.select(avg("TotalSpent")).collect()[0][0]
print(f"Gasto promedio por cliente: ${avg_spend:,.2f}")
resultados["07_gasto_promedio_cliente"] = pd.DataFrame({"GastoPromedio": [round(float(avg_spend), 2)]})

print("\n" + "=" * 60)
print("8. MÍNIMO, MÁXIMO Y PROMEDIO DE CANTIDAD POR TRANSACCIÓN")
print("=" * 60)
min_qty = df.select(spark_min("Quantity")).collect()[0][0]
max_qty = df.select(spark_max("Quantity")).collect()[0][0]
avg_qty = df.select(avg("Quantity")).collect()[0][0]
print(f"Mínimo: {min_qty}, Máximo: {max_qty}, Promedio: {avg_qty:.2f}")
resultados["08_cantidad_transaccion"] = pd.DataFrame({
    "Minimo": [min_qty],
    "Maximo": [max_qty],
    "Promedio": [round(float(avg_qty), 2)]
})

print("\n" + "=" * 60)
print("9. MES DEL AÑO CON MÁS VENTAS")
print("=" * 60)
df_with_date = df_with_revenue.withColumn("InvoiceDate", to_date(col("InvoiceDate"), "M/d/yyyy H:mm"))
df_with_month = df_with_date.filter(col("InvoiceDate").isNotNull()).withColumn("Month", month("InvoiceDate"))
df_with_year = df_with_month.withColumn("Year", year("InvoiceDate"))
monthly_sales = df_with_year.groupBy("Year", "Month").agg(sum("Revenue").alias("TotalSales")) \
    .orderBy(desc("TotalSales"))
top_month = monthly_sales.first()
print(f"Mes con más ventas: {top_month['Month']}/{top_month['Year']}")
print(f"Ventas: ${top_month['TotalSales']:,.2f}")
resultados["09_ventas_por_mes"] = pd.DataFrame({
    "Year": [top_month['Year']],
    "Month": [top_month['Month']],
    "TotalSales": [round(float(top_month['TotalSales']), 2)]
})

print("\n" + "=" * 60)
print("10. PORCENTAJE DE REGISTROS CON DEVOLUCIONES")
print("=" * 60)
total_reg = df.count()
reg_with_returns = df.filter(col("Quantity") < 0).count()
percentage_returns = (reg_with_returns / total_reg) * 100
print(f"Registros con devoluciones: {reg_with_returns}")
print(f"Porcentaje: {percentage_returns:.2f}%")
resultados["10_porcentaje_devoluciones"] = pd.DataFrame({
    "TotalRegistros": [total_reg],
    "RegistrosDevolucion": [reg_with_returns],
    "Porcentaje": [round(float(percentage_returns), 2)]
})

print("\n" + "=" * 60)
print("EXPORTANDO RESULTADOS")
print("=" * 60)

import os
os.makedirs("resultados", exist_ok=True)

for filename, df_result in resultados.items():
    df_result.to_csv(f"resultados/{filename}.csv", index=False)
    print(f"Guardado: resultados/{filename}.csv")

resumen_data = {
    "Pregunta": [
        "1. Total Registros",
        "2. Clientes Unicos",
        "3. Ingreso Total",
        "4. Producto Mas Vendido",
        "5. Top Cliente",
        "6. Top 5 Paises sin UK",
        "7. Gasto Promedio Cliente",
        "8. Cantidad por Transaccion",
        "9. Mes con Mas Ventas",
        "10. Porcentaje Devoluciones"
    ],
    "Resultado": [
        str(total_registros),
        str(unique_customers),
        f"${total_revenue:,.2f}",
        f"{top_product['Description']} ({top_product['TotalQuantity']} unidades)",
        f"Cliente {int(top_customer['CustomerID'])} (${top_customer['TotalSpent']:,.2f})",
        "Netherlands, EIRE, Germany, France, Australia",
        f"${avg_spend:,.2f}",
        f"Min: {min_qty}, Max: {max_qty}, Prom: {avg_qty:.2f}",
        f"{top_month['Month']}/{top_month['Year']} (${top_month['TotalSales']:,.2f})",
        f"{percentage_returns:.2f}%"
    ]
}
df_resumen = pd.DataFrame(resumen_data)
df_resumen.to_csv("resultados/resumen_general.csv", index=False)
print("Guardado: resultados/resumen_general.csv")

spark.stop()
print("\n" + "=" * 60)
print("ANÁLISIS COMPLETADO")
print("=" * 60)
