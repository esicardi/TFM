# Databricks notebook source
# MAGIC %md
# MAGIC Calculo la curva de incidencia acumulada y el corredor endémico. Salvo los datos en la capa gold.

# COMMAND ----------

from pymongo import MongoClient
import pandas as pd
import numpy as np

# URL de conexión MongoDB
mongo_url = "mongodb+srv://myAtlasDBUser:Pregunta42_@myatlasclusteredu.x8divu1.mongodb.net/?retryWrites=true&w=majority&tls=true"

# Nombre de la base de datos y la colección
database_name = "vigilancia"
collection_name = "casos_seguimiento"

# Conectar a MongoDB
client = MongoClient(mongo_url)

# Seleccionar la base de datos
db = client[database_name]

# Seleccionar la colección
collection = db[collection_name]

# Filtrar los casos por evento, AE, y estado confirmado
filtro = {
    'evento': 'Leptospirosis',
    'estado': 'confirmado',
    'AE': {'$gte': 2019, '$lte': 2023}
}

# Obtener los casos de MongoDB
casos = list(collection.find(filtro, {'AE': 1, 'SE': 1}))

# Crear un diccionario para almacenar los casos acumulados por AE y SE
acumulados_por_ae = {ae: [0]*53 for ae in range(2019, 2024)}

# Acumular los casos confirmados por SE para cada AE
for caso in casos:
    ae = caso['AE']
    se = caso['SE']
    if 1 <= se <= 53:
        acumulados_por_ae[ae][se-1] += 1

# Hacer la suma acumulada para cada AE
for ae in acumulados_por_ae:
    acumulados_por_ae[ae] = np.cumsum(acumulados_por_ae[ae])

# Mostrar los resultados
for ae, acumulados in acumulados_por_ae.items():
    print(f"AE {ae}: {acumulados}")


# COMMAND ----------

# MAGIC %md
# MAGIC Figura de Corredor endemico de leptospirosis

# COMMAND ----------

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import gmean
from scipy import stats


# Convertir el diccionario en un DataFrame para facilidad
df_acumulados = pd.DataFrame(acumulados_por_ae)

# Calcular la media geométrica y el IC para cada SE
media_geom = []
ci_left = []
ci_right = []

for se in range(53):
    # Casos acumulados por SE (ignoramos SEs sin casos)
    casos_se = df_acumulados.iloc[se].values
    if np.all(casos_se == 0):  # Si no hay casos para esa SE
        media_geom.append(0)
        ci_left.append(0)
        ci_right.append(0)
    else:
        # Media geométrica
        media = gmean(casos_se)
        media_geom.append(media)

        # Log-transformación para IC
        log_values = np.log(casos_se[casos_se > 0])  # Ignorar valores cero
        mean_log = np.mean(log_values)
        se_log = np.std(log_values, ddof=1) / np.sqrt(len(log_values))

        # Intervalos de confianza
        ci_log_left = mean_log - 1.96 * se_log
        ci_log_right = mean_log + 1.96 * se_log

        ci_left.append(np.exp(ci_log_left))
        ci_right.append(np.exp(ci_log_right))

# Crear un DataFrame con los resultados
df_resultados = pd.DataFrame({
    'SE': range(1, 54),
    'Media Geometrica': media_geom,
    'IC Izquierdo': ci_left,
    'IC Derecho': ci_right
})

# Graficar las medias geométricas e IC
plt.figure(figsize=(10, 6))

# Graficar las áreas según los criterios de color con las fronteras en gris
plt.fill_between(df_resultados['SE'], 0, df_resultados['IC Izquierdo'], color='green', edgecolor='grey')  # Verde por debajo del IC izquierdo
plt.fill_between(df_resultados['SE'], df_resultados['IC Izquierdo'], df_resultados['Media Geometrica'], color='yellow', edgecolor='grey')  # Amarillo entre IC izquierdo y media geométrica
plt.fill_between(df_resultados['SE'], df_resultados['Media Geometrica'], df_resultados['IC Derecho'], color='orange', edgecolor='grey')  # Naranja entre media geométrica y IC derecho
plt.fill_between(df_resultados['SE'], df_resultados['IC Derecho'], max(df_resultados['IC Derecho']) + 5, color='red', edgecolor='grey')  # Rojo por encima del IC derecho

# Graficar las curvas de la media geométrica y los IC
plt.plot(df_resultados['SE'], df_resultados['Media Geometrica'], color='black',linewidth=0.5, linestyle='--')
plt.plot(df_resultados['SE'], df_resultados['IC Izquierdo'], color='black',linewidth=0.5, linestyle='--')
plt.plot(df_resultados['SE'], df_resultados['IC Derecho'], color='black', linewidth=0.5,linestyle='--')

# Configurar el gráfico sin leyenda y con las fronteras en gris
plt.title('Curva de Medias Geométricas e Intervalos de Confianza (IC) para Incidencia no Acumulada')
plt.xlabel('Semana Epidemiológica (SE)')
plt.ylabel('Media Geométrica de Casos no Acumulados')
plt.grid(True)

# Mostrar el gráfico
plt.show()

# COMMAND ----------

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient
from scipy.stats import gmean
from scipy import stats

# Conectar a MongoDB
mongo_url = "mongodb+srv://myAtlasDBUser:Pregunta42_@myatlasclusteredu.x8divu1.mongodb.net/?retryWrites=true&w=majority&tls=true"
client = MongoClient(mongo_url)
database_name = "vigilancia"
collection_name = "casos_seguimiento"
db = client[database_name]
collection = db[collection_name]

# Filtrar los casos para el año 2024
filtro_2024 = {
    'evento': 'Leptospirosis',
    'estado': 'confirmado',
    'AE': 2024
}

# Obtener los casos de MongoDB
casos_2024 = list(collection.find(filtro_2024, {'SE': 1}))

# Crear un diccionario para almacenar los casos acumulados por SE
acumulados_2024 = [0] * 53

# Acumular los casos confirmados por SE para el año 2024
for caso in casos_2024:
    se = caso['SE']
    if 1 <= se <= 53:
        acumulados_2024[se - 1] += 1

# Crear un DataFrame con los resultados de acumulados para el año 2024
df_acumulados_2024 = pd.DataFrame({
    'SE': range(1, 54),
    'Acumulados 2024': np.cumsum(acumulados_2024)
})

# Graficar las medias geométricas e IC
plt.figure(figsize=(10, 6))

# Graficar las áreas según los criterios de color con las fronteras en gris
plt.fill_between(df_resultados['SE'], 0, df_resultados['IC Izquierdo'], color='green', edgecolor='grey')  # Verde por debajo del IC izquierdo
plt.fill_between(df_resultados['SE'], df_resultados['IC Izquierdo'], df_resultados['Media Geometrica'], color='yellow', edgecolor='grey')  # Amarillo entre IC izquierdo y media geométrica
plt.fill_between(df_resultados['SE'], df_resultados['Media Geometrica'], df_resultados['IC Derecho'], color='orange', edgecolor='grey')  # Naranja entre media geométrica y IC derecho
plt.fill_between(df_resultados['SE'], df_resultados['IC Derecho'], max(df_resultados['IC Derecho']) + 5, color='red', edgecolor='grey')  # Rojo por encima del IC derecho

# Graficar las curvas de la media geométrica y los IC
plt.plot(df_resultados['SE'], df_resultados['Media Geometrica'], color='black', linewidth=0.5, linestyle='--')
plt.plot(df_resultados['SE'], df_resultados['IC Izquierdo'], color='black', linewidth=0.5, linestyle='--')
plt.plot(df_resultados['SE'], df_resultados['IC Derecho'], color='black', linewidth=0.5, linestyle='--')

# Graficar la curva de casos acumulados para 2024 en negro ancho 2
plt.plot(df_acumulados_2024['SE'], df_acumulados_2024['Acumulados 2024'], color='black', linewidth=2, label='Casos Acumulados 2024')

# Configurar el gráfico sin leyenda y con las fronteras en gris
plt.title('Curva de Medias Geométricas e Intervalos de Confianza (IC) para Incidencia no Acumulada y Casos Acumulados 2024')
plt.xlabel('Semana Epidemiológica (SE)')
plt.ylabel('Número de Casos')
plt.grid(True)

# Mostrar el gráfico
plt.show()


# COMMAND ----------

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient
from scipy.stats import gmean

# Conectar a MongoDB
mongo_url = "mongodb+srv://myAtlasDBUser:Pregunta42_@myatlasclusteredu.x8divu1.mongodb.net/?retryWrites=true&w=majority&tls=true"
client = MongoClient(mongo_url)
database_name = "vigilancia"
collection_name = "casos_seguimiento"
db = client[database_name]
collection = db[collection_name]

# Filtrar los casos para el año 2024
filtro_2024 = {
    'evento': 'Leptospirosis',
    'estado': 'confirmado',
    'AE': 2024
}

# Obtener los casos de MongoDB
casos_2024 = list(collection.find(filtro_2024, {'SE': 1}))

# Crear un diccionario para almacenar los casos acumulados por SE
acumulados_2024 = [0] * 53

# Acumular los casos confirmados por SE para el año 2024
for caso in casos_2024:
    se = caso['SE']
    if 1 <= se <= 53:
        acumulados_2024[se - 1] += 1

# Identificar la última SE con datos
ultima_se_con_datos = max([caso['SE'] for caso in casos_2024], default=53)

# Crear un DataFrame con los resultados de acumulados para el año 2024
df_acumulados_2024 = pd.DataFrame({
    'SE': range(1, ultima_se_con_datos + 1),
    'Acumulados 2024': np.cumsum(acumulados_2024[:ultima_se_con_datos])
})

# Calcular la media geométrica y el IC para cada SE
media_geom = []
ci_left = []
ci_right = []

for se in range(53):
    # Casos acumulados por SE (ignoramos SEs sin casos)
    casos_se = df_acumulados.iloc[se].values
    if np.all(casos_se == 0):  # Si no hay casos para esa SE
        media_geom.append(0)
        ci_left.append(0)
        ci_right.append(0)
    else:
        # Media geométrica
        media = gmean(casos_se)
        media_geom.append(media)

        # Log-transformación para IC
        log_values = np.log(casos_se[casos_se > 0])  # Ignorar valores cero
        mean_log = np.mean(log_values)
        se_log = np.std(log_values, ddof=1) / np.sqrt(len(log_values))

        # Intervalos de confianza
        ci_log_left = mean_log - 1.96 * se_log
        ci_log_right = mean_log + 1.96 * se_log

        ci_left.append(np.exp(ci_log_left))
        ci_right.append(np.exp(ci_log_right))

# Crear un DataFrame con los resultados
df_resultados = pd.DataFrame({
    'SE': range(1, 54),
    'Media Geométrica': media_geom,
    'IC Izquierdo': ci_left,
    'IC Derecho': ci_right
})

# Graficar las medias geométricas e IC
plt.figure(figsize=(10, 6))

# Graficar las áreas según los criterios de color con las fronteras en gris
plt.fill_between(df_resultados['SE'], 0, df_resultados['IC Izquierdo'], color='green', edgecolor='grey')  # Verde por debajo del IC izquierdo
plt.fill_between(df_resultados['SE'], df_resultados['IC Izquierdo'], df_resultados['Media Geométrica'], color='yellow', edgecolor='grey')  # Amarillo entre IC izquierdo y media geométrica
plt.fill_between(df_resultados['SE'], df_resultados['Media Geométrica'], df_resultados['IC Derecho'], color='orange', edgecolor='grey')  # Naranja entre media geométrica y IC derecho
plt.fill_between(df_resultados['SE'], df_resultados['IC Derecho'], max(df_resultados['IC Derecho']) + 5, color='red', edgecolor='grey')  # Rojo por encima del IC derecho

# Graficar las curvas de la media geométrica y los IC
plt.plot(df_resultados['SE'], df_resultados['Media Geométrica'], color='black', linewidth=0.5, linestyle='--')
plt.plot(df_resultados['SE'], df_resultados['IC Izquierdo'], color='black', linewidth=0.5, linestyle='--')
plt.plot(df_resultados['SE'], df_resultados['IC Derecho'], color='black', linewidth=0.5, linestyle='--')

# Graficar la curva de casos acumulados para 2024 en negro ancho 2 hasta la última SE con datos
plt.plot(df_acumulados_2024['SE'], df_acumulados_2024['Acumulados 2024'], color='black', linewidth=2, label='Casos Acumulados 2024')

# Configurar el gráfico sin leyenda y con las fronteras en gris
plt.title('Curva de Medias Geométricas e Intervalos de Confianza (IC) para Incidencia no Acumulada y Casos Acumulados 2024')
plt.xlabel('Semana Epidemiológica (SE)')
plt.ylabel('Número de Casos')
plt.grid(True)

# Mostrar el gráfico
plt.show()


# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Almacenamiento
account = "stmd01estrella"
spark.conf.set(f"fs.azure.account.key.{account}.dfs.core.windows.net","aO0SDuNHT7RQFaCaMN+YiW2R5829YhVT2GtOey+Utaw611i/J+b300eZMYSAfVbNeMz8U/X94aVC+AStgeMROw==")




# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import re

# Crear sesión de Spark y configurar Delta Lake
spark = SparkSession.builder \
    .appName("Exportar a Delta") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Configurar acceso a Azure Data Lake
account = "stmd01estrella"
spark.conf.set(f"fs.azure.account.key.{account}.dfs.core.windows.net", "aO0SDuNHT7RQFaCaMN+YiW2R5829YhVT2GtOey+Utaw611i/J+b300eZMYSAfVbNeMz8U/X94aVC+AStgeMROw==")

# Define el contenedor y la ruta base en Azure Data Lake
container = "gold"  # Cambia a tu contenedor real si es necesario
ruta_base = f"abfss://{container}@{account}.dfs.core.windows.net/corredor_lepto/"

# Asume que tienes los DataFrames df_resultados y df_acumulados_2024 ya creados


def clean_column_names(df):
    # Define a function to clean column names
    def clean_column_name(col_name):
        # Replace spaces with underscores and remove other invalid characters
        return re.sub(r'[ ,;{}()\n\t=]+', '_', col_name)
    
    # Apply the cleaning function to each column name
    return df.toDF(*[clean_column_name(c) for c in df.columns])

# Clean the column names of your DataFrames
df_resultados_spark = clean_column_names(spark.createDataFrame(df_resultados))
df_acumulados_2024_spark = clean_column_names(spark.createDataFrame(df_acumulados_2024))

# Now you can proceed with writing these DataFrames to Delta format as before
df_resultados_spark.write.format("delta").mode("overwrite").save(ruta_base + "/corredor_2019_2024")
df_acumulados_2024_spark.write.format("delta").mode("overwrite").save(ruta_base + "/acumulados_2024")

# Leer los datos guardados para verificación
df_corredor_2019_2024 = spark.read.format("delta").load(ruta_base + "corredor_2019_2024")
df_acumulados_2024 = spark.read.format("delta").load(ruta_base + "acumulados_2024")

# Mostrar los primeros registros para comprobar que se exportaron correctamente
df_corredor_2019_2024.show()
df_acumulados_2024.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Genero la vista de los datos para poder importarlos de PowerBI

# COMMAND ----------

# Registrar los DataFrames como vistas temporales
df_resultados_spark.createOrReplaceTempView("corredor_2019_2024")
df_acumulados_2024_spark.createOrReplaceTempView("acumulados_2024")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- Crear tabla permanente para df_resultados
# MAGIC CREATE TABLE IF NOT EXISTS corredor_2019_2024
# MAGIC USING DELTA
# MAGIC AS SELECT * FROM corredor_2019_2024;
# MAGIC
# MAGIC -- Crear tabla permanente para df_acumulados_2024
# MAGIC CREATE TABLE IF NOT EXISTS acumulados_2024
# MAGIC USING DELTA
# MAGIC AS SELECT * FROM acumulados_2024;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM corredor_2019_2024 LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM acumulados_2024 LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %pip install openpyxl

# COMMAND ----------

# MAGIC %md
# MAGIC A partir de la coleccion casos_seguimiento de MongoDB, busco los casos de Dengue confirmados, y comparo las coordenadas de la dirección del caso con las coordenadas de los polígonos (cuadrados) del mapa (solo para el rango de poligonos que cae sobre Uruguay). Si ese poligono tenia marcado un 0, como ausencia de dengue, voy a ver si los casos nuevos confirmados, alguno cae dentro. Si es asi actualizo a 1. Los que estaban 1 los dejo igual, porque son poligonos donde ya historicamente se constato la presencia de dengue.

# COMMAND ----------

import pymongo
import pandas as pd
from pymongo import MongoClient
import shutil
# URL de conexión MongoDB
mongo_url = "mongodb+srv://myAtlasDBUser:Pregunta42_@myatlasclusteredu.x8divu1.mongodb.net/?retryWrites=true&w=majority&tls=true"

# Nombre de la base de datos y la colección
database_name = "vigilancia"
collection_name = "casos_seguimiento"

# Conectar a MongoDB
client = MongoClient(mongo_url)

# Seleccionar la base de datos
db = client[database_name]

# Seleccionar la colección
collection = db[collection_name]

# Buscar casos confirmados con evento dengue
dengue_cases = list(collection.find({
    "estado": "confirmado",
    "evento": "Dengue"
}))

# Cargar archivos Excel desde Databricks File System (DBFS)
mi_archivo_path = "/Workspace/Users/estrellasicardi@udelar409.onmicrosoft.com/mi_archivo.xlsx"
coordenadas_puntos_path = "/Workspace/Users/estrellasicardi@udelar409.onmicrosoft.com/coordenadas_puntos.xlsx"

# Cargar archivos Excel utilizando 'with open'
with open(mi_archivo_path, 'rb') as mi_archivo, open(coordenadas_puntos_path, 'rb') as coordenadas_puntos:
    # Leer los datos desde los archivos Excel
    datos_originales = pd.read_excel(mi_archivo)
    coordenadas_puntos = pd.read_excel(coordenadas_puntos)

# Crear una copia de la tabla de datos originales para actualizaciones
datos_actualizados = datos_originales.copy()

# Iterar sobre cada FID en el rango entre 5251 y 5668
for fid in range(5251, 5669):
    # Obtener las coordenadas del centroide para este FID
    centroide = coordenadas_puntos[coordenadas_puntos['FID'] == fid]
    
    if centroide.empty:
        continue
    
    lat_centroide = centroide['ycoord'].values[0]
    lon_centroide = centroide['xcoord'].values[0]
    print("FID:", fid)
    print("lat:", lat_centroide)
    print("lon:", lon_centroide)
    # Verificar si algún caso de dengue confirmado cae dentro del rango de ±0.5° del centroide
    for case in dengue_cases:
        lat_case = case['latitud']
        lon_case = case['longitud']
        print(lat_case)
        print(lon_case)
        
        # Comprobar si la latitud y longitud están dentro del rango de ±0.5°
        if (lat_centroide - 0.25 <= lat_case <= lat_centroide + 0.25) and (lon_centroide - 0.25 <= lon_case <= lon_centroide + 0.25):
            # Si el valor de dengue_SA es 0, cambiarlo a 1
            index_fid = datos_actualizados[datos_actualizados['FID'] == fid].index
            if not datos_actualizados.loc[index_fid, 'dengue_SA'].values[0]:
                datos_actualizados.loc[index_fid, 'dengue_SA'] = 1

# Guardar los datos actualizados en un nuevo archivo Excel en DBFS
# Use a local path for the temporary Excel file
local_output_path = "/tmp/datos_actualizados.xlsx"
output_dbfs_path = "/Workspace/Users/estrellasicardi@udelar409.onmicrosoft.com/datos_actualizados.xlsx"

# Save the DataFrame to a local Excel file
datos_actualizados.to_excel(local_output_path, index=False)

# Move the local file to DBFS
with open(local_output_path, 'rb') as temp_output_file, open(output_dbfs_path, 'wb') as dbfs_output_file:
    shutil.copyfileobj(temp_output_file, dbfs_output_file)

print("Proceso completado. Los datos actualizados han sido guardados en:", output_dbfs_path)

