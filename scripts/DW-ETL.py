import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import time

start_time = time.time()
#Extracción (Cargar el CSV)
data_path = "sales.csv"
df = pd.read_csv(data_path, low_memory=False)

#Limpieza, elimina todas las filas que contengan al menos una columna con valor nulo
df = df.dropna()

#Creación de columnas nuevas
df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y')
df['Year'] = df['Date'].dt.year
df['Month'] = df['Date'].dt.month
df['Day of the week'] = df['Date'].dt.day_name()
df['Profit'] = (df['State Bottle Retail'] - df['State Bottle Cost']) * df['Bottles Sold']
df['Price per Milliliter'] = df['State Bottle Retail'] / df['Bottle Volume (ml)']

#Transformación

#Seleccionar columnas importantes para tablas de hechos y de dimensiones
#Tabla Hechos Ventas por tienda 
ventas_tienda_hechos = df[['Invoice/Item Number',
                        'Date',
                        'Store Number',
                        'Bottles Sold',
                        'Sale (Dollars)',
                        'Volume Sold (Liters)']]
#Tabla dimensión precio
dim_precio = df[['Invoice/Item Number',
                'State Bottle Cost',
                'State Bottle Retail',
                'Price per Milliliter']]
#Tabla dimensión Producto_ventas
dim_producto_ventas = df[['Invoice/Item Number',
                'Item Description',
                'Category',
                'Category Name',
                'Vendor Number',
                'Vendor Name',
                'Pack',
                'Bottle Volume (ml)']]
#Tabla dimensión Ubicación
dim_ubicacion = df[['Store Number',
                'Store Name',
                'Address',
                'City',
                'Zip Code',
                'Store Location',
                'County Number',
                'County']]
#Tabla dimensión Date
dim_date = df[['Date',
            'Year',
            'Month',
            'Day of the week']]


#Tabla de hechos Costos
costos_hechos = df[['Invoice/Item Number',
            'Profit',
            'Bottle Volume (ml)',
            'Date']]
#Tabla Dimensión Proveedor
dim_proveedor = df[['Vendor Number',
                'Vendor Name']].drop_duplicates()
#Tabla Dimensión Producto_Costos
dim_producto_costos = df[['Invoice/Item Number',
            'Item Description',
            'Pack']]
#Tabla Dimensión Tiempo
dim_tiempo = df[['Date',
            'Year',
            'Month',
            'Day of the week']]
#Tabla Dimensión Categoría
dim_categoría = df[['Category',
            'Category Name',
            'Pack']]



#Load Cargar tablas a sqlite
conn = sqlite3.connect("data_warehouse.db")
#Cargar cada tabla en sqlite
ventas_tienda_hechos.to_sql("Ventas_Tienda", conn, if_exists="replace", index=False)
dim_precio.to_sql("Dimension_Precio", conn, if_exists="replace", index=False)
dim_producto_ventas.to_sql("Dimension_Producto_Ventas", conn, if_exists="replace", index=False)
dim_ubicacion.to_sql("Dimension_Ubicacion", conn, if_exists="replace", index=False)
dim_date.to_sql("Dimension_Date", conn, if_exists="replace", index=False)
costos_hechos.to_sql("Costos_Hechos", conn, if_exists="replace", index=False)
dim_proveedor.to_sql("Dimension_Proveedor", conn, if_exists="replace", index=False)
dim_producto_costos.to_sql("Dimension_Producto_Costos", conn, if_exists="replace", index=False)
dim_categoría.to_sql("Dimension_Categoria", conn, if_exists="replace", index=False)

endtime = time.time()
execution_time = endtime - start_time
print(f"ETL completado con una duración de {execution_time:.4f}, datos almacenados en SQLite en data_warehouse.db")


cursor = conn.cursor()

# Consultas de ventas por region
query_ventas_region = """
SELECT du."City", SUM(vt."Sale (Dollars)") AS Total_Ventas
FROM Ventas_Tienda AS vt
JOIN Dimension_Ubicacion AS du ON vt."Store Number" = du."Store Number" 
GROUP BY du."City"
ORDER BY Total_Ventas DESC
LIMIT 5;
"""

query_ventas_tienda = """
SELECT vt."Store Number", du."Store Name", SUM(vt."Sale (Dollars)") as Total_Ventas
FROM Ventas_Tienda as vt
JOIN Dimension_Ubicacion as du ON vt."Store Number" = du."Store Number" 
GROUP BY vt."Store Number"
ORDER BY Total_Ventas DESC
LIMIT 5;
"""

query_costos_producto = """
SELECT dpv."Item Description", SUM(ch."Profit") as Total_Costos
FROM Costos_Hechos as ch
JOIN Dimension_Producto_Costos as dpv ON ch."Invoice/Item Number" = dpv."Invoice/Item Number"
GROUP BY dpv."Item Description"
ORDER BY Total_Costos DESC
LIMIT 5;
"""

# Consulta de ventas por mes
query_ventas_mes = """
SELECT strftime('%m', vt."Date") AS Month, SUM(vt."Sale (Dollars)") as Total_Ventas
FROM Ventas_Tienda as vt
GROUP BY Month
ORDER BY Month;
"""

# Consulta de ventas por mes
query_ventas_precio = """
SELECT pv."Item Description", dp."Price per Milliliter" AS Precio_Mililitro, SUM(vt."Sale (Dollars)") as Total_Ventas
FROM Ventas_Tienda as vt
JOIN Dimension_Producto_Ventas AS pv ON pv."Invoice/Item Number" = vt."Invoice/Item Number"
JOIN Dimension_Precio AS dp ON dp."Invoice/Item Number" = vt."Invoice/Item Number"
GROUP BY Precio_Mililitro
ORDER BY Total_Ventas DESC
LIMIT 10;
"""

query_porcentaje_ganancia = """
SELECT AVG(((dp."State Bottle Retail" - dp."State Bottle Cost") / dp."State Bottle Cost") * 100) AS Porcentaje_Ganancia_Promedio
FROM Dimension_Precio AS dp;
"""

query_costo_promedio_ano = """
SELECT strftime('%Y', vt."Date") AS Year, AVG(dp."State Bottle Cost") AS Costo_Promedio
FROM Ventas_Tienda AS vt
JOIN Dimension_Precio AS dp ON vt."Invoice/Item Number" = dp."Invoice/Item Number"
GROUP BY Year
ORDER BY Year;
"""

# Ejecutar las consultas
ventas_region_df = pd.read_sql_query(query_ventas_region, conn)
ventas_tienda_df = pd.read_sql_query(query_ventas_tienda, conn)
costos_producto_df = pd.read_sql_query(query_costos_producto, conn)
ventas_mes_df = pd.read_sql_query(query_ventas_mes, conn)
porcentaje_ganancia_df = pd.read_sql_query(query_porcentaje_ganancia, conn)
costo_promedio_ano_df = pd.read_sql_query(query_costo_promedio_ano, conn)
ventas_precio_df = pd.read_sql_query(query_ventas_precio, conn)


# Graficación
# Crear las subgráficas
fig, axs = plt.subplots(2, 2, figsize=(14, 10))

# Cambiar el color de fondo de la figura
fig.patch.set_facecolor('#073763')  # Fondo de la figura (gráfico completo)

# Cambiar el color de fondo de cada subgráfico
axs[0,0].set_facecolor('#073763')  # Fondo del  gráfico
axs[0,1].set_facecolor('#073763')  # Fondo del  gráfico
axs[1,0].set_facecolor('#073763')  # Fondo del  gráfico
axs[1,1].set_facecolor('#073763')  # Fondo del  gráfico

# 1. Gráfica de ventas por tienda
axs[0, 0].bar(ventas_tienda_df['Store Name'], ventas_tienda_df['Total_Ventas'], color='#edd0c5')
axs[0, 0].set_title("Top 5 Tiendas por Ventas", color="#d3c79b")
axs[0, 0].set_xlabel("Tienda", color="#d3c79b")
axs[0, 0].set_ylabel("Ventas (Dólares)", color="#d3c79b")
axs[0, 0].tick_params(axis='x', labelcolor="#d3c79b", rotation=45)
axs[0, 0].tick_params(axis='y', labelcolor="#d3c79b")
axs[0, 0].grid(True)

# 2. Gráfica de costos por producto
axs[0, 1].bar(costo_promedio_ano_df['Year'], costo_promedio_ano_df['Costo_Promedio'], color='#edd0c5')
axs[0, 1].set_title("Evolucion de Costos", color="#d3c79b")
axs[0, 1].set_xlabel("Ano", color="#d3c79b")
axs[0, 1].set_ylabel("Costos (Dólares)", color="#d3c79b")
axs[0, 1].tick_params(axis='x', labelcolor="#d3c79b", rotation=45)
axs[0, 1].tick_params(axis='y', labelcolor="#d3c79b")
axs[0, 1].grid(True)

# 3. Gráfica de ventas por Region
axs[1, 0].bar(ventas_region_df['City'], ventas_region_df['Total_Ventas'], color='#edd0c5')
axs[1, 0].set_title("Ventas por Region", color="#d3c79b")
axs[1, 0].set_xlabel("Region", color="#d3c79b")
axs[1, 0].set_ylabel("Ventas (Dólares)", color="#d3c79b")
axs[1, 0].tick_params(axis='x', labelcolor="#d3c79b", rotation=45)
axs[1, 0].tick_params(axis='y', labelcolor="#d3c79b")
axs[1, 0].grid(True)

# 4. Gráfica de ventas por mes
axs[1, 1].plot(ventas_mes_df['Month'], ventas_mes_df['Total_Ventas'], marker='o', color='#edd0c5')
axs[1, 1].set_title("Ventas Totales por Mes", color="#d3c79b")
axs[1, 1].set_xlabel("Mes", color="#d3c79b")
axs[1, 1].set_ylabel("Ventas (Dólares)", color="#d3c79b")
axs[1, 1].tick_params(axis='x', labelcolor="#d3c79b", rotation=45)
axs[1, 1].tick_params(axis='y', labelcolor="#d3c79b")
axs[1, 1].grid(True)

# Ajustar el espacio entre las subgráficas
plt.tight_layout()

# Mostrar las gráficas
plt.show()

fig, ax = plt.subplots(1, 2, figsize=(14, 6))

# Cambiar el color de fondo de la figura
fig.patch.set_facecolor('#073763')  # Fondo de la figura (gráfico completo)

# Cambiar el color de fondo de cada subgráfico
ax[0].set_facecolor('#073763')  # Fondo del primer gráfico
ax[1].set_facecolor('#073763')  # Fondo del segundo gráfico

# Primer gráfico: Ventas por producto
ax[0].plot(ventas_precio_df['Item Description'], ventas_precio_df['Total_Ventas'], marker='o', linestyle='-', color='#edd0c5')
ax[0].set_title('Ventas por Producto', color= "#d3c79b")
ax[0].grid(True)
ax[0].tick_params(axis='x', labelcolor="#d3c79b", rotation=45)  # Rotar las etiquetas del eje X
ax[0].tick_params(axis='y', labelcolor="#d3c79b")

# Segundo gráfico: Calidad de Producto (Precio por Mililitro)
ax[1].plot(ventas_precio_df['Item Description'], ventas_precio_df['Precio_Mililitro'], marker='o', linestyle='-', color='#edd0c5')
ax[1].set_title('Calidad de Producto (Precio por Mililitro)', color = "#d3c79b")
ax[1].grid(True)
ax[1].tick_params(axis='x', labelcolor="#d3c79b", rotation=45)  # Rotar las etiquetas del eje X
ax[1].tick_params(axis='y', labelcolor="#d3c79b")

# Ajustar el diseño y mostrar el gráfico
plt.tight_layout()
plt.show()

print(porcentaje_ganancia_df)