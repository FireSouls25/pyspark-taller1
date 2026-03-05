from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, avg, min as spark_min, max as spark_max, count,
    when, month, year, desc, asc, upper, lower, trim, 
    rank, row_number, expr, round
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

print("=" * 60)
print("LECTURA DE DATOS")
print("=" * 60)

pandas_df = pd.read_excel("online+retail/Online Retail.xlsx")
pandas_df['CustomerID'] = pandas_df['CustomerID'].fillna(0)
df = spark.createDataFrame(pandas_df)

print(f"Total de registros: {df.count()}")
print("Esquema del DataFrame:")
df.printSchema()

df = df.withColumn("Quantity", col("Quantity").cast(IntegerType()))
df = df.withColumn("UnitPrice", col("UnitPrice").cast(DoubleType()))

print("\n" + "=" * 60)
print("1. NÚMERO TOTAL DE FACTURAS")
print("=" * 60)
total_invoices = df.select("InvoiceNo").distinct().count()
print(f"Total de facturas únicas: {total_invoices}")
df.select("InvoiceNo").distinct().coalesce(1).write.csv("resultados/01_total_facturas.csv", header=True, mode="overwrite")

print("\n" + "=" * 60)
print("2. NÚMERO DE CLIENTES ÚNICOS")
print("=" * 60)
unique_customers = df.select("CustomerID").distinct().count()
print(f"Total de clientes únicos: {unique_customers}")
df.select("CustomerID").distinct().coalesce(1).write.csv("resultados/02_clientes_unicos.csv", header=True, mode="overwrite")

print("\n" + "=" * 60)
print("3. INGRESO TOTAL (Quantity * UnitPrice)")
print("=" * 60)
df_with_revenue = df.withColumn("Revenue", col("Quantity") * col("UnitPrice"))
total_revenue = df_with_revenue.select(sum("Revenue")).collect()[0][0]
print(f"Ingreso total: ${total_revenue:,.2f}")
df_with_revenue.select(sum("Revenue")).coalesce(1).write.csv("resultados/03_ingreso_total.csv", header=True, mode="overwrite")

print("\n" + "=" * 60)
print("4. PRODUCTO MÁS VENDIDO EN CANTIDAD")
print("=" * 60)
top_product = df.groupBy("Description").agg(sum("Quantity").alias("TotalQuantity")) \
    .orderBy(desc("TotalQuantity")).first()
print(f"Producto más vendido: {top_product['Description']}")
print(f"Cantidad total: {top_product['TotalQuantity']}")
df.groupBy("Description").agg(sum("Quantity").alias("TotalQuantity")) \
    .orderBy(desc("TotalQuantity")).coalesce(1).write.csv("resultados/04_productos_mas_vendidos.csv", header=True, mode="overwrite")

print("\n" + "=" * 60)
print("5. CLIENTE CON MAYOR VOLUMEN DE COMPRA")
print("=" * 60)
df_with_revenue_clean = df_with_revenue.filter(col("CustomerID") != 0)
top_customer = df_with_revenue_clean.groupBy("CustomerID").agg(sum("Revenue").alias("TotalSpent")) \
    .orderBy(desc("TotalSpent")).first()
print(f"Cliente con mayor volumen: {int(top_customer['CustomerID'])}")
print(f"Monto total: ${top_customer['TotalSpent']:,.2f}")
df_with_revenue_clean.groupBy("CustomerID").agg(sum("Revenue").alias("TotalSpent")) \
    .orderBy(desc("TotalSpent")).coalesce(1).write.csv("resultados/05_top_clientes_gasto.csv", header=True, mode="overwrite")

print("\n" + "=" * 60)
print("6. 5 PAÍSES QUE MÁS COMPRAN FUERA DE REINO UNIDO")
print("=" * 60)
non_uk_sales = df_with_revenue.filter(col("Country") != "United Kingdom") \
    .groupBy("Country").agg(round(sum("Revenue"), 2).alias("TotalSales")) \
    .orderBy(desc("TotalSales")).limit(5)
print("Top 5 países (sin UK):")
non_uk_sales.show()
non_uk_sales.coalesce(1).write.csv("resultados/06_top_paises_sin_uk.csv", header=True, mode="overwrite")

print("\n" + "=" * 60)
print("7. TICKET PROMEDIO POR FACTURA")
print("=" * 60)
invoice_totals = df_with_revenue.groupBy("InvoiceNo").agg(
    sum("Revenue").alias("InvoiceTotal")
)
avg_invoice = invoice_totals.select(avg("InvoiceTotal")).collect()[0][0]
print(f"Ticket promedio por factura: ${avg_invoice:,.2f}")
invoice_totals.coalesce(1).write.csv("resultados/07_ticket_promedio.csv", header=True, mode="overwrite")

print("\n" + "=" * 60)
print("8. MÍNIMO, MÁXIMO Y PROMEDIO DE PRODUCTOS POR FACTURA")
print("=" * 60)
products_per_invoice = df.groupBy("InvoiceNo").agg(count("Quantity").alias("ProductCount"))
min_products = products_per_invoice.select(spark_min("ProductCount")).collect()[0][0]
max_products = products_per_invoice.select(spark_max("ProductCount")).collect()[0][0]
avg_products = products_per_invoice.select(avg("ProductCount")).collect()[0][0]
print(f"Mínimo productos por factura: {min_products}")
print(f"Máximo productos por factura: {max_products}")
print(f"Promedio productos por factura: {avg_products:.2f}")
products_per_invoice.coalesce(1).write.csv("resultados/08_productos_por_factura.csv", header=True, mode="overwrite")

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
monthly_sales.coalesce(1).write.csv("resultados/09_ventas_por_mes.csv", header=True, mode="overwrite")

print("\n" + "=" * 60)
print("10. PORCENTAJE DE FACTURAS CON DEVOLUCIONES")
print("=" * 60)
total_invoices = df.select("InvoiceNo").distinct().count()
invoices_with_returns = df.filter(col("Quantity") < 0).select("InvoiceNo").distinct().count()
percentage_returns = (invoices_with_returns / total_invoices) * 100
print(f"Facturas con devoluciones: {invoices_with_returns}")
print(f"Porcentaje de facturas con devoluciones: {percentage_returns:.2f}%")
spark.createDataFrame([(total_invoices, invoices_with_returns, percentage_returns)]) \
    .toDF("TotalFacturas", "FacturasDevolucion", "Porcentaje") \
    .coalesce(1).write.csv("resultados/10_porcentaje_devoluciones.csv", header=True, mode="overwrite")

print("\n" + "=" * 60)
print("OPERACIONES ADICIONALES")
print("=" * 60)

print("\n--- SELECCIÓN DE COLUMNAS ---")
df.select("InvoiceNo", "Description", "Quantity", "UnitPrice", "Country").show(5)

print("\n--- FILTRADO DE DATOS ---")
df.filter(col("Country") == "Germany").show(5)

print("\n--- ORDENAMIENTO ---")
df_with_revenue.groupBy("InvoiceNo").agg(round(sum("Revenue"), 2).alias("Total")) \
    .orderBy(desc("Total")).show(5, truncate=False)

print("\n--- AGRUPACIÓN CON AGG ---")
df.groupBy("Country").agg(
    count("InvoiceNo").alias("NumFacturas"),
    sum("Quantity").alias("CantidadTotal"),
    round(avg("UnitPrice"), 2).alias("PrecioPromedio")
).orderBy(desc("NumFacturas")).show(5)

print("\n--- CREACIÓN DE COLUMNAS DERIVADAS ---")
df_with_category = df.withColumn(
    "IsReturn", when(col("Quantity") < 0, "Yes").otherwise("No")
).withColumn(
    "Revenue", round(col("Quantity") * col("UnitPrice"), 2)
).withColumn(
    "CountryUpper", upper(col("Country"))
)
df_with_category.select("InvoiceNo", "Description", "Quantity", "IsReturn", "Revenue", "CountryUpper").show(5, truncate=False)

print("\n--- UNIONES (JOINS) ---")
invoices_df = df.select("InvoiceNo", "Country").distinct()
revenue_df = df_with_revenue.groupBy("InvoiceNo").agg(round(sum("Revenue"), 2).alias("TotalRevenue"))
joined_df = invoices_df.join(revenue_df, "InvoiceNo")
joined_df.show(5, truncate=False)

print("\n--- FUNCIONES DE VENTANA (RANKING) ---")
customer_ranking = df_with_revenue_clean.groupBy("CustomerID").agg(
    round(sum("Revenue"), 2).alias("TotalSpent")
).withColumn(
    "Rank", row_number().over(Window.orderBy(desc("TotalSpent")))
).orderBy("Rank")
customer_ranking.show(10, truncate=False)

print("\n--- EXPORTACIÓN FINAL ---")
customer_ranking.coalesce(1).write.csv("resultados/ranking_clientes.csv", header=True, mode="overwrite")

spark.stop()
print("\n" + "=" * 60)
print("ANÁLISIS COMPLETADO")
print("=" * 60)
