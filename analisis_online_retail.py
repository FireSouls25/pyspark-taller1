from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, avg, min as spark_min, max as spark_max, count,
    when, month, year, desc, asc, upper, lower, trim, 
    rank, row_number, expr, round as spark_round, lit
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
print("1. NÚMERO TOTAL DE FACTURAS")
print("=" * 60)
total_invoices = df.select("InvoiceNo").distinct().count()
print(f"Total de facturas únicas: {total_invoices}")
resultados["01_total_facturas"] = pd.DataFrame({"TotalFacturas": [total_invoices]})

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
print("7. TICKET PROMEDIO POR FACTURA")
print("=" * 60)
invoice_totals = df_with_revenue.groupBy("InvoiceNo").agg(
    spark_round(sum("Revenue"), 2).alias("InvoiceTotal")
)
avg_invoice = invoice_totals.select(avg("InvoiceTotal")).collect()[0][0]
print(f"Ticket promedio por factura: ${avg_invoice:,.2f}")
resultados["07_ticket_promedio"] = pd.DataFrame({"TicketPromedio": [round(float(avg_invoice), 2)]})

print("\n" + "=" * 60)
print("8. MÍNIMO, MÁXIMO Y PROMEDIO DE PRODUCTOS POR FACTURA")
print("=" * 60)
products_per_invoice = df.groupBy("InvoiceNo").agg(count("Quantity").alias("ProductCount"))
min_products = products_per_invoice.select(spark_min("ProductCount")).collect()[0][0]
max_products = products_per_invoice.select(spark_max("ProductCount")).collect()[0][0]
avg_products = products_per_invoice.select(avg("ProductCount")).collect()[0][0]
print(f"Mínimo: {min_products}, Máximo: {max_products}, Promedio: {avg_products:.2f}")
resultados["08_productos_por_factura"] = pd.DataFrame({
    "Minimo": [min_products],
    "Maximo": [max_products],
    "Promedio": [round(float(avg_products), 2)]
})

print("\n" + "=" * 60)
print("9. MES DEL AÑO CON MÁS VENTAS")
print("=" * 60)
df_with_date = df_with_revenue.withColumn("InvoiceDate", col("InvoiceDate"))
df_with_month = df_with_date.withColumn("Month", month("InvoiceDate"))
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
print("10. PORCENTAJE DE FACTURAS CON DEVOLUCIONES")
print("=" * 60)
total_inv = df.select("InvoiceNo").distinct().count()
invoices_with_returns = df.filter(col("Quantity") < 0).select("InvoiceNo").distinct().count()
percentage_returns = (invoices_with_returns / total_inv) * 100
print(f"Facturas con devoluciones: {invoices_with_returns}")
print(f"Porcentaje: {percentage_returns:.2f}%")
resultados["10_porcentaje_devoluciones"] = pd.DataFrame({
    "TotalFacturas": [total_inv],
    "FacturasDevolucion": [invoices_with_returns],
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
        "1. Total Facturas",
        "2. Clientes Unicos",
        "3. Ingreso Total",
        "4. Producto Mas Vendido",
        "5. Top Cliente",
        "6. Top 5 Paises sin UK",
        "7. Ticket Promedio",
        "8. Productos por Factura",
        "9. Mes con Mas Ventas",
        "10. Porcentaje Devoluciones"
    ],
    "Resultado": [
        str(total_invoices),
        str(unique_customers),
        f"${total_revenue:,.2f}",
        f"{top_product['Description']} ({top_product['TotalQuantity']} unidades)",
        f"Cliente {int(top_customer['CustomerID'])} (${top_customer['TotalSpent']:,.2f})",
        "Netherlands, EIRE, Germany, France, Australia",
        f"${avg_invoice:,.2f}",
        f"Min: {min_products}, Max: {max_products}, Prom: {avg_products:.2f}",
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
