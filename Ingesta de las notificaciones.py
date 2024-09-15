# Databricks notebook source
# MAGIC %md
# MAGIC Configuracion de Almacenamiento

# COMMAND ----------

# Almacenamiento
account = "stmd01estrella"
spark.conf.set(f"fs.azure.account.key.{account}.dfs.core.windows.net","aO0SDuNHT7RQFaCaMN+YiW2R5829YhVT2GtOey+Utaw611i/J+b300eZMYSAfVbNeMz8U/X94aVC+AStgeMROw==")

# COMMAND ----------

# MAGIC %md
# MAGIC Configuracion de Kafka

# COMMAND ----------

def read_config():
  config = {}
  with open("/Workspace/FileStore/client.properties") as fh:
    for line in fh:
      line = line.strip()
      if len(line) != 0 and line[0] != "#":
        parameter, value = line.strip().split('=', 1)
        config[parameter] = value.strip()
  return config

conf = read_config()

# COMMAND ----------

#configuracion
topic1 = "notif_dengue"
topic2="notif_leptospirosis"
topic3="notif_mening_viral"
topic4="notif_mening_bact"
datasource = "eventos"
dataset1 = topic1
dataset2 = topic2
dataset3 = topic3
dataset4 = topic4

bronze_path = f"abfss://bronze@{account}.dfs.core.windows.net"

# COMMAND ----------

def read_config():
  config = {}
  with open("/Workspace/FileStore/client.properties") as fh:
    for line in fh:
      line = line.strip()
      if len(line) != 0 and line[0] != "#":
        parameter, value = line.strip().split('=', 1)
        config[parameter] = value.strip()
  return config

conf = read_config()

# COMMAND ----------

print(conf)

# COMMAND ----------

# creamos un streaming dataframe
df1 = (spark
      .readStream
      .format("kafka") 
      .option("kafka.bootstrap.servers",conf["bootstrap.servers"])
      .option("kafka.security.protocol", conf["security.protocol"])
      .option("kafka.sasl.mechanism", conf["sasl.mechanisms"])
      .option("kafka.sasl.jaas.config", 
              f"""kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{conf.get('sasl.username')}" password="{conf.get('sasl.password')}"; """)
      .option("subscribe",topic1)
      .option("startingOffsets", "earliest")
      .load()
)

# COMMAND ----------

# creamos un streaming dataframe
df2 = (spark
      .readStream
      .format("kafka") 
      .option("kafka.bootstrap.servers",conf["bootstrap.servers"])
      .option("kafka.security.protocol", conf["security.protocol"])
      .option("kafka.sasl.mechanism", conf["sasl.mechanisms"])
      .option("kafka.sasl.jaas.config", 
              f"""kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{conf.get('sasl.username')}" password="{conf.get('sasl.password')}"; """)
      .option("subscribe",topic2)
      .option("startingOffsets", "earliest")
      .load()
)

# COMMAND ----------

# creamos un streaming dataframe
df3 = (spark
      .readStream
      .format("kafka") 
      .option("kafka.bootstrap.servers",conf["bootstrap.servers"])
      .option("kafka.security.protocol", conf["security.protocol"])
      .option("kafka.sasl.mechanism", conf["sasl.mechanisms"])
      .option("kafka.sasl.jaas.config", 
              f"""kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{conf.get('sasl.username')}" password="{conf.get('sasl.password')}"; """)
      .option("subscribe",topic3)
      .option("startingOffsets", "earliest")
      .load()
)

# COMMAND ----------

# creamos un streaming dataframe
df4 = (spark
      .readStream
      .format("kafka") 
      .option("kafka.bootstrap.servers",conf["bootstrap.servers"])
      .option("kafka.security.protocol", conf["security.protocol"])
      .option("kafka.sasl.mechanism", conf["sasl.mechanisms"])
      .option("kafka.sasl.jaas.config", 
              f"""kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{conf.get('sasl.username')}" password="{conf.get('sasl.password')}"; """)
      .option("subscribe",topic4)
      .option("startingOffsets", "earliest")
      .load()
)

# COMMAND ----------

display(df1)

# COMMAND ----------

display(df2)

# COMMAND ----------

display(df3)

# COMMAND ----------

display(df4)

# COMMAND ----------

# Lista todas las columnas del DataFrame
columnas = df1.columns

# Imprime la lista de columnas
print("Columnas en el DataFrame:", columnas)

# Verifica si la columna 'value' está presente
if 'value' in columnas:
    print("La columna 'value' está presente.")
else:
    print("La columna 'value' NO está presente.")


# COMMAND ----------

# creamos un streaming dataframe
df = (spark
      .readStream
      .format("kafka") 
      .option("kafka.bootstrap.servers",conf["bootstrap.servers"])
      .option("kafka.security.protocol", conf["security.protocol"])
      .option("kafka.sasl.mechanism", conf["sasl.mechanisms"])
      .option("kafka.sasl.jaas.config", 
              f"""kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{conf.get('sasl.username')}" password="{conf.get('sasl.password')}"; """)
      .option("subscribe",topic)
      .option("startingOffsets", "earliest")
      .load()
)

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import col

# Crear un streaming DataFrame leyendo desde Kafka
df1 = (spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", conf["bootstrap.servers"])
      .option("kafka.security.protocol", conf["security.protocol"])
      .option("kafka.sasl.mechanism", conf["sasl.mechanisms"])
      .option("kafka.sasl.jaas.config", 
              f"""kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{conf.get('sasl.username')}" password="{conf.get('sasl.password')}"; """)
      .option("subscribe", topic1)
      .option("startingOffsets", "earliest")
      .load()
)

# Convertir las columnas key y value de binario a texto
df1 = df1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Ahora puedes trabajar con `key` y `value` como columnas de texto
df1.printSchema()  # Muestra el esquema para verificar que son columnas de tipo string


# COMMAND ----------

display(df1)

# COMMAND ----------

from pyspark.sql.functions import col

# Crear un streaming DataFrame leyendo desde Kafka
df2 = (spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", conf["bootstrap.servers"])
      .option("kafka.security.protocol", conf["security.protocol"])
      .option("kafka.sasl.mechanism", conf["sasl.mechanisms"])
      .option("kafka.sasl.jaas.config", 
              f"""kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{conf.get('sasl.username')}" password="{conf.get('sasl.password')}"; """)
      .option("subscribe", topic2)
      .option("startingOffsets", "earliest")
      .load()
)

# Convertir las columnas key y value de binario a texto
df2 = df2.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Ahora puedes trabajar con `key` y `value` como columnas de texto
df2.printSchema()  # Muestra el esquema para verificar que son columnas de tipo string

# COMMAND ----------

from pyspark.sql.functions import col

# Crear un streaming DataFrame leyendo desde Kafka
df3 = (spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", conf["bootstrap.servers"])
      .option("kafka.security.protocol", conf["security.protocol"])
      .option("kafka.sasl.mechanism", conf["sasl.mechanisms"])
      .option("kafka.sasl.jaas.config", 
              f"""kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{conf.get('sasl.username')}" password="{conf.get('sasl.password')}"; """)
      .option("subscribe", topic3)
      .option("startingOffsets", "earliest")
      .load()
)

# Convertir las columnas key y value de binario a texto
df3 = df3.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Ahora puedes trabajar con `key` y `value` como columnas de texto
df1.printSchema()  # Muestra el esquema para verificar que son columnas de tipo string

# COMMAND ----------

from pyspark.sql.functions import col

# Crear un streaming DataFrame leyendo desde Kafka
df4 = (spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", conf["bootstrap.servers"])
      .option("kafka.security.protocol", conf["security.protocol"])
      .option("kafka.sasl.mechanism", conf["sasl.mechanisms"])
      .option("kafka.sasl.jaas.config", 
              f"""kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{conf.get('sasl.username')}" password="{conf.get('sasl.password')}"; """)
      .option("subscribe", topic4)
      .option("startingOffsets", "earliest")
      .load()
)

# Convertir las columnas key y value de binario a texto
df4 = df4.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Ahora puedes trabajar con `key` y `value` como columnas de texto
df1.printSchema()  # Muestra el esquema para verificar que son columnas de tipo string

# COMMAND ----------

display(df2)

# COMMAND ----------

display(df3)

# COMMAND ----------

display(df4)

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col

# Definir el esquema de las columnas basado en el diccionario proporcionado
schema1 = StructType([
    StructField("nombre", StringType(), True),
    StructField("apellido", StringType(), True),
    StructField("direccion", StringType(), True),
    StructField("localidad", StringType(), True),
    StructField("departamento", StringType(), True),
    StructField("latitud", FloatType(), True),
    StructField("longitud", FloatType(), True),
    StructField("cedula", StringType(), True),
    StructField("fecha_nacimiento", StringType(), True),
    StructField("fecha_inicio_sintomas", StringType(), True),
    StructField("edad", IntegerType(), True),
    StructField("sexo", StringType(), True),
    StructField("evento", StringType(), True),
    StructField("telefono", StringType(), True),
    StructField("celular", StringType(), True),
    StructField("prestador", StringType(), True),
    StructField("internado", StringType(), True),
    StructField("tipo_internado", StringType(), True),
    StructField("fiebre", StringType(), True),
    StructField("dolor_abdominal_intenso_continuo", StringType(), True),
    StructField("cefalea", StringType(), True),
    StructField("edemas", StringType(), True),
    StructField("dolor_retroorbitario", StringType(), True),
    StructField("sangrado_mucosas", StringType(), True),
    StructField("nauseas_vomitos", StringType(), True),
    StructField("sangrado_grave", StringType(), True),
    StructField("exantema", StringType(), True),
    StructField("letargia_irritabilidad", StringType(), True),
    StructField("mialgias_artralgias", StringType(), True),
    StructField("shock", StringType(), True),
    StructField("hematocrito", IntegerType(), True),
    StructField("plaquetas", IntegerType(), True),
    StructField("leucocitos_sangre", IntegerType(), True),
    StructField("timestamp", TimestampType(), True) 
])


# Deserializar la columna 'value' que contiene el JSON en un DataFrame
df1_parsed = df1.withColumn("data", from_json(col("value"), schema1)).select("key", "data.*")

# Muestra el esquema del nuevo DataFrame
df1_parsed.printSchema()

# Ahora puedes trabajar con las columnas individuales


# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col

# Definir el esquema de las columnas basado en el diccionario proporcionado
schema2 = StructType([
    StructField("nombre", StringType(), True),
    StructField("apellido", StringType(), True),
    StructField("direccion", StringType(), True),
    StructField("localidad", StringType(), True),
    StructField("departamento", StringType(), True),
    StructField("latitud", FloatType(), True),
    StructField("longitud", FloatType(), True),
    StructField("cedula", StringType(), True),
    StructField("fecha_nacimiento", StringType(), True),
    StructField("fecha_inicio_sintomas", StringType(), True),
    StructField("edad", IntegerType(), True),
    StructField("sexo", StringType(), True),
    StructField("evento", StringType(), True),
    StructField("telefono", StringType(), True),
    StructField("celular", StringType(), True),
    StructField("prestador", StringType(), True),
    StructField("internado", StringType(), True),
    StructField("tipo_internado", StringType(), True),
    StructField("fiebre", StringType(), True),
    StructField("cefalea", StringType(), True),
    StructField("sindrome_hemorragiparo", StringType(), True),
    StructField("mialgias_artralgias", StringType(), True),
    StructField("compromiso_renal", StringType(), True),
    StructField("compromiso_hepatico", StringType(), True),
    StructField("sintomatologia_digestiva", StringType(), True),
    StructField("ictericia", StringType(), True),
    StructField("hiperemia_conjuntival", StringType(), True),
    StructField("neumonia_neumonitis", StringType(), True),
    StructField("lesiones_piel", StringType(), True),
    StructField("hematocrito", IntegerType(), True),
    StructField("plaquetas", IntegerType(), True),
    StructField("leucocitos_sangre", IntegerType(), True),
    StructField("funcion_hepatica_bt", FloatType(), True),
    StructField("funcion_hepatica_tgo", FloatType(), True),
    StructField("funcion_hepatica_ldh", FloatType(), True),
    StructField("funcion_hepatica_tgp", FloatType(), True),
    StructField("funcion_renal_azoemia", FloatType(), True),
    StructField("funcion_renal_creatinemia", FloatType(), True),
    StructField("timestamp", TimestampType(), True) 
])



# Deserializar la columna 'value' que contiene el JSON en un DataFrame
df2_parsed = df2.withColumn("data", from_json(col("value"), schema2)).select("key", "data.*")

# Muestra el esquema del nuevo DataFrame
df2_parsed.printSchema()

# Ahora puedes trabajar con las columnas individuales


# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col

# Definir el esquema de las columnas basado en el diccionario proporcionado
schema3 = StructType([
    StructField("nombre", StringType(), True),
    StructField("apellido", StringType(), True),
    StructField("direccion", StringType(), True),
    StructField("localidad", StringType(), True),
    StructField("departamento", StringType(), True),
    StructField("latitud", FloatType(), True),
    StructField("longitud", FloatType(), True),
    StructField("cedula", StringType(), True),
    StructField("fecha_nacimiento", StringType(), True),
    StructField("fecha_inicio_sintomas", StringType(), True),
    StructField("edad", IntegerType(), True),
    StructField("sexo", StringType(), True),
    StructField("evento", StringType(), True),
    StructField("telefono", StringType(), True),
    StructField("celular", StringType(), True),
    StructField("prestador", StringType(), True),
    StructField("internado", StringType(), True),
    StructField("tipo_internado", StringType(), True),
    StructField("fiebre", StringType(), True),
    StructField("convulsion", StringType(), True),
    StructField("petequias", StringType(), True),
    StructField("signos_irritacion_meningea", StringType(), True),
    StructField("alteracion_conciencia", StringType(), True),
    StructField("hipopefusion_periferica", StringType(), True),
    StructField("paciente_grave", StringType(), True),
    StructField("directo_lcr", StringType(), True),
    StructField("aspecto_lcr_citoquimico", StringType(), True),
    StructField("glucosa_lcr", FloatType(), True),
    StructField("proteinas_lcr", FloatType(), True),
    StructField("leucocitos_lcr", IntegerType(), True),
    StructField("predominio", StringType(), True),
    StructField("timestamp", TimestampType(), True) 
])


# Deserializar la columna 'value' que contiene el JSON en un DataFrame
df3_parsed = df3.withColumn("data", from_json(col("value"), schema3)).select("key", "data.*")

# Muestra el esquema del nuevo DataFrame
df3_parsed.printSchema()

# Ahora puedes trabajar con las columnas individuales

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col

# Definir el esquema de las columnas basado en el diccionario proporcionado
schema4 = StructType([
    StructField("nombre", StringType(), True),
    StructField("apellido", StringType(), True),
    StructField("direccion", StringType(), True),
    StructField("localidad", StringType(), True),
    StructField("departamento", StringType(), True),
    StructField("latitud", FloatType(), True),
    StructField("longitud", FloatType(), True),
    StructField("cedula", StringType(), True),
    StructField("fecha_nacimiento", StringType(), True),
    StructField("fecha_inicio_sintomas", StringType(), True),
    StructField("edad", IntegerType(), True),
    StructField("sexo", StringType(), True),
    StructField("evento", StringType(), True),
    StructField("telefono", StringType(), True),
    StructField("celular", StringType(), True),
    StructField("prestador", StringType(), True),
    StructField("internado", StringType(), True),
    StructField("tipo_internado", StringType(), True),
    StructField("fiebre", StringType(), True),
    StructField("convulsion", StringType(), True),
    StructField("petequias", StringType(), True),
    StructField("signos_irritacion_meningea", StringType(), True),
    StructField("alteracion_conciencia", StringType(), True),
    StructField("hipopefusion_periferica", StringType(), True),
    StructField("paciente_grave", StringType(), True),
    StructField("directo_lcr", StringType(), True),
    StructField("aspecto_lcr_citoquimico", StringType(), True),
    StructField("glucosa_lcr", FloatType(), True),
    StructField("proteinas_lcr", FloatType(), True),
    StructField("leucocitos_lcr", IntegerType(), True),
    StructField("predominio", StringType(), True),
    StructField("timestamp", TimestampType(), True) 
])


# Deserializar la columna 'value' que contiene el JSON en un DataFrame
df4_parsed = df4.withColumn("data", from_json(col("value"), schema4)).select("key", "data.*")

# Muestra el esquema del nuevo DataFrame
df4_parsed.printSchema()

# COMMAND ----------

display(df1_parsed)

# COMMAND ----------

display(df2_parsed)

# COMMAND ----------

display(df3_parsed)

# COMMAND ----------

display(df4_parsed)

# COMMAND ----------


bronzeTableLocation = f"{bronze_path}/{datasource}/{dataset1}/"
bronzeCheckPointLocation = f"{bronze_path}/{datasource}/{dataset1}/_checkpoint"

(df1_parsed.writeStream 
  .format("delta")
  .trigger(availableNow=True)
  .option("mergeSchema", "true")
  .option("checkpointLocation", bronzeCheckPointLocation)
  .start(bronzeTableLocation)
)

# COMMAND ----------

bronzeTableLocation = f"{bronze_path}/{datasource}/{dataset2}/"
bronzeCheckPointLocation = f"{bronze_path}/{datasource}/{dataset2}/_checkpoint"

(df2_parsed.writeStream 
  .format("delta")
  .trigger(availableNow=True)
  .option("mergeSchema", "true")
  .option("checkpointLocation", bronzeCheckPointLocation)
  .start(bronzeTableLocation)
)

# COMMAND ----------

bronzeTableLocation = f"{bronze_path}/{datasource}/{dataset3}/"
bronzeCheckPointLocation = f"{bronze_path}/{datasource}/{dataset3}/_checkpoint"

(df3_parsed.writeStream 
  .format("delta")
  .trigger(availableNow=True)
  .option("mergeSchema", "true")
  .option("checkpointLocation", bronzeCheckPointLocation)
  .start(bronzeTableLocation)
)

# COMMAND ----------

bronzeTableLocation = f"{bronze_path}/{datasource}/{dataset4}/"
bronzeCheckPointLocation = f"{bronze_path}/{datasource}/{dataset4}/_checkpoint"

(df4_parsed.writeStream 
  .format("delta")
  .trigger(availableNow=True)
  .option("mergeSchema", "true")
  .option("checkpointLocation", bronzeCheckPointLocation)
  .start(bronzeTableLocation)
)

# COMMAND ----------

display(dbutils.fs.ls(bronzeTableLocation))

# COMMAND ----------

import pyspark.sql.functions as F
bronzeTableLocation1 = f"{bronze_path}/{datasource}/{dataset1}/"
df_1 = (spark.read.format("delta")
        .load(bronzeTableLocation1)
        .orderBy(F.col("timestamp").desc())
        .limit(100))
        
display(df_1)

# COMMAND ----------

import pyspark.sql.functions as F
bronzeTableLocation2 = f"{bronze_path}/{datasource}/{dataset2}/"
df_2 = (spark.read.format("delta")
        .load(bronzeTableLocation2)
        .orderBy(F.col("timestamp").desc())
        .limit(100))
        
display(df_2)

# COMMAND ----------

import pyspark.sql.functions as F
bronzeTableLocation3 = f"{bronze_path}/{datasource}/{dataset3}/"
df_3 = (spark.read.format("delta")
        .load(bronzeTableLocation3)
        .orderBy(F.col("timestamp").desc())
        .limit(100))
        
display(df_3)

# COMMAND ----------

import pyspark.sql.functions as F
bronzeTableLocation4 = f"{bronze_path}/{datasource}/{dataset4}/"
df_4 = (spark.read.format("delta")
        .load(bronzeTableLocation4)
        .orderBy(F.col("timestamp").desc())
        .limit(100))
        
display(df_4)

# COMMAND ----------

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import urllib.parse

# Escapar el nombre de usuario y la contraseña
username = urllib.parse.quote_plus("myAtlasDBUser")
password = urllib.parse.quote_plus("Pregunta42_")

# Construir el URI correctamente escapado
uri = f"mongodb+srv://{username}:{password}@myatlasclusteredu.x8divu1.mongodb.net/?retryWrites=true&w=majority&tls=true"

# Crear un nuevo cliente y conectarse al servidor
client = MongoClient(uri, server_api=ServerApi('1'))

# Enviar un ping para confirmar una conexión exitosa
try:
    client.admin.command('ping')
    print("¡Conexión exitosa a MongoDB!")
except Exception as e:
    print(f"Error al conectar: {e}")



# COMMAND ----------


bronzeTableLocation1 = f"{bronze_path}/{datasource}/{dataset1}/"
bronzeCheckPointLocation1 = f"{bronze_path}/{datasource}/{dataset1}/_checkpoint"
bronzeTableLocation2 = f"{bronze_path}/{datasource}/{dataset2}/"
bronzeCheckPointLocation2 = f"{bronze_path}/{datasource}/{dataset2}/_checkpoint"
bronzeTableLocation3 = f"{bronze_path}/{datasource}/{dataset3}/"
bronzeCheckPointLocation3 = f"{bronze_path}/{datasource}/{dataset3}/_checkpoint"
bronzeTableLocation4 = f"{bronze_path}/{datasource}/{dataset4}/"
bronzeCheckPointLocation4 = f"{bronze_path}/{datasource}/{dataset4}/_checkpoint"

# COMMAND ----------

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Configura el URI de conexión a MongoDB Atlas
mongo_uri = "mongodb+srv://myAtlasDBUser:Pregunta42_@myatlasclusteredu.x8divu1.mongodb.net/?retryWrites=true&w=majority&tls=true"
database_name = "vigilancia"  # Cambia a tu base de datos
collection_name = "notificaciones_eventos"  # Cambia a tu colección

# Crea la sesión de Spark con la configuración para MongoDB
spark = SparkSession.builder \
    .appName("MongoDB Integration") \
    .config("spark.mongodb.output.uri", mongo_uri) \
    .config("spark.mongodb.output.database", database_name) \
    .config("spark.mongodb.output.collection", collection_name) \
    .getOrCreate()

# Configura el DataFrame que deseas escribir
df01 = (spark.read.format("delta")
        .load(bronzeTableLocation1)
        .orderBy(F.col("timestamp").desc())
        .limit(200))

# Configura las opciones de conexión
write_config = {
    "uri": mongo_uri,
    "database": database_name,
    "collection": collection_name,
    "writeConcern.w": "majority"
}

# Escribe el DataFrame en MongoDB Atlas
df01.write \
    .format("mongo") \
    .mode("append") \
    .options(**write_config) \
    .save()


# COMMAND ----------

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Configura el URI de conexión a MongoDB Atlas
mongo_uri = "mongodb+srv://myAtlasDBUser:Pregunta42_@myatlasclusteredu.x8divu1.mongodb.net/?retryWrites=true&w=majority&tls=true"
database_name = "vigilancia"  # Cambia a tu base de datos
collection_name = "notificaciones_eventos"  # Cambia a tu colección

# Crea la sesión de Spark con la configuración para MongoDB
spark = SparkSession.builder \
    .appName("MongoDB Integration") \
    .config("spark.mongodb.output.uri", mongo_uri) \
    .config("spark.mongodb.output.database", database_name) \
    .config("spark.mongodb.output.collection", collection_name) \
    .getOrCreate()

# Configura el DataFrame que deseas escribir
df02 = (spark.read.format("delta")
        .load(bronzeTableLocation2)
        .orderBy(F.col("timestamp").desc())
        .limit(200))

# Configura las opciones de conexión
write_config = {
    "uri": mongo_uri,
    "database": database_name,
    "collection": collection_name,
    "writeConcern.w": "majority"
}

# Escribe el DataFrame en MongoDB Atlas
df02.write \
    .format("mongo") \
    .mode("append") \
    .options(**write_config) \
    .save()


# COMMAND ----------

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Configura el URI de conexión a MongoDB Atlas
mongo_uri = "mongodb+srv://myAtlasDBUser:Pregunta42_@myatlasclusteredu.x8divu1.mongodb.net/?retryWrites=true&w=majority&tls=true"
database_name = "vigilancia"  # Cambia a tu base de datos
collection_name = "notificaciones_eventos"  # Cambia a tu colección

# Crea la sesión de Spark con la configuración para MongoDB
spark = SparkSession.builder \
    .appName("MongoDB Integration") \
    .config("spark.mongodb.output.uri", mongo_uri) \
    .config("spark.mongodb.output.database", database_name) \
    .config("spark.mongodb.output.collection", collection_name) \
    .getOrCreate()

# Configura el DataFrame que deseas escribir
df03 = (spark.read.format("delta")
        .load(bronzeTableLocation3)
        .orderBy(F.col("timestamp").desc())
        .limit(200))

# Configura las opciones de conexión
write_config = {
    "uri": mongo_uri,
    "database": database_name,
    "collection": collection_name,
    "writeConcern.w": "majority"
}

# Escribe el DataFrame en MongoDB Atlas
df03.write \
    .format("mongo") \
    .mode("append") \
    .options(**write_config) \
    .save()


# COMMAND ----------

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Configura el URI de conexión a MongoDB Atlas
mongo_uri = "mongodb+srv://myAtlasDBUser:Pregunta42_@myatlasclusteredu.x8divu1.mongodb.net/?retryWrites=true&w=majority&tls=true"
database_name = "vigilancia"  # Cambia a tu base de datos
collection_name = "notificaciones_eventos"  # Cambia a tu colección

# Crea la sesión de Spark con la configuración para MongoDB
spark = SparkSession.builder \
    .appName("MongoDB Integration") \
    .config("spark.mongodb.output.uri", mongo_uri) \
    .config("spark.mongodb.output.database", database_name) \
    .config("spark.mongodb.output.collection", collection_name) \
    .getOrCreate()

# Configura el DataFrame que deseas escribir
df04 = (spark.read.format("delta")
        .load(bronzeTableLocation4)
        .orderBy(F.col("timestamp").desc())
        .limit(200))

# Configura las opciones de conexión
write_config = {
    "uri": mongo_uri,
    "database": database_name,
    "collection": collection_name,
    "writeConcern.w": "majority"
}

# Escribe el DataFrame en MongoDB Atlas
df04.write \
    .format("mongo") \
    .mode("append") \
    .options(**write_config) \
    .save()


# COMMAND ----------

import socket

def check_port(host, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    try:
        sock.connect((host, port))
        print(f"Puerto {port} está abierto en {host}")
    except socket.error as e:
        print(f"Error al conectar al puerto {port} en {host}: {e}")
    finally:
        sock.close()

# Reemplaza con la IP y el puerto del servidor MongoDB Atlas
check_port('myatlasclusteredu.x8divu1.mongodb.net', 27017)


# COMMAND ----------

spark.version

