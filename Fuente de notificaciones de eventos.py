# Databricks notebook source
# MAGIC %md
# MAGIC Primero leo la configuracion de Kafka
# MAGIC
# MAGIC

# COMMAND ----------

from confluent_kafka import Producer

# Configuración de Kafka
kafka_bootstrap_servers = "pkc-56d1g.eastus.azure.confluent.cloud:9092"
kafka_security_protocol = "SASL_SSL"
kafka_sasl_mechanism = "PLAIN"
kafka_sasl_username = 'FSTWSMEA7VVOLI3V'
kafka_sasl_password = 'T00hs+03Dcdt7dRmPG0QvpJ5ZjYeSW34YEMa4KroUSbaOCJWGxPv0SiOVg99C9gq'

# Configuraciones de Kafka para el productor de mensajes
kafka_options = {
    "bootstrap.servers": kafka_bootstrap_servers,
    "security.protocol": kafka_security_protocol,
    "sasl.mechanism": kafka_sasl_mechanism,
    "sasl.username": kafka_sasl_username,
    "sasl.password": kafka_sasl_password,
    "session.timeout.ms": 45000  # Opcional, si deseas ajustar el tiempo de sesión
}

# Crear una instancia del productor de Kafka
producer = Producer(kafka_options)

# Lista de topics a los que se enviarán mensajes
topics = ["notif_leptospirosis", "notif_dengue", "notif_mening_viral", "notif_mening_bact"]

# COMMAND ----------

# MAGIC %md
# MAGIC Creo los topics
# MAGIC

# COMMAND ----------

from confluent_kafka.admin import NewTopic

# Lista de los topics a crear
topics = ["notif_leptospirosis", "notif_dengue", "notif_mening_viral", "notif_mening_bact"]

# Configuración de particiones y factor de replicación
num_partitions = 3
replication_factor = 3

# Crear objetos NewTopic para cada topic
new_topics = [NewTopic(topic, num_partitions=num_partitions, replication_factor=replication_factor) for topic in topics]

# Crear los topics
fs = admin_client.create_topics(new_topics)

# Esperamos a que termine la operación
for topic, f in fs.items():
    try:
        f.result()  # The result itself is None
        print(f"Topic {topic} creado")
    except Exception as e:
        print(f"Error al crear el topic {topic}: {e}")


# COMMAND ----------

# MAGIC %md
# MAGIC Listo topics en Kafka

# COMMAND ----------

topic_list = admin_client.list_topics().topics 
print(f"Topics en el cluster de Kafka:\n{topic_list}") 

# COMMAND ----------

# MAGIC %md
# MAGIC Leo archivo de datos, y separo el json segun evento
# MAGIC

# COMMAND ----------

import json
import pandas as pd

# Ruta al archivo JSON
json_path = "/Workspace/Users/estrellasicardi@udelar409.onmicrosoft.com/notificaciones_ficticias.json"

# Leer el archivo JSON
with open(json_path, encoding='utf-8') as f:
    data = json.load(f)

# Inicializar listas vacías para cada evento
meningitis_viral = []
meningitis_bacteriana = []
leptospirosis = []
dengue = []

# Separar los registros según el valor de "evento"
for registro in data:
    if registro["evento"] == "Meningitis Viral":
        meningitis_viral.append(registro)
    elif registro["evento"] == "Meningitis Bacteriana":
        meningitis_bacteriana.append(registro)
    elif registro["evento"] == "Leptospirosis":
        leptospirosis.append(registro)
    elif registro["evento"] == "Dengue":
        dengue.append(registro)

# Mostrar el conteo de registros en cada lista
print(f"Meningitis Viral: {len(meningitis_viral)} registros")
print(f"Meningitis Bacteriana: {len(meningitis_bacteriana)} registros")
print(f"Leptospirosis: {len(leptospirosis)} registros")
print(f"Dengue: {len(dengue)} registros")

# Convertir las listas a DataFrames si es necesario
df_meningitis_viral = pd.DataFrame(meningitis_viral)
df_meningitis_bacteriana = pd.DataFrame(meningitis_bacteriana)
df_leptospirosis = pd.DataFrame(leptospirosis)
df_dengue = pd.DataFrame(dengue)

# Mostrar los primeros registros de cada DataFrame
print("\nMeningitis Viral:")
print(df_meningitis_viral.head())

print("\nMeningitis Bacteriana:")
print(df_meningitis_bacteriana.head())

print("\nLeptospirosis:")
print(df_leptospirosis.head())

print("\nDengue:")
print(df_dengue.head())


# COMMAND ----------

# MAGIC %md
# MAGIC Genero una tabla Spark para cada evento

# COMMAND ----------

from pyspark.sql import SparkSession
import pandas as pd

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("Eventos Notificacion Obligatoria") \
    .getOrCreate()

# Convertir los DataFrames de pandas a DataFrames de Spark
spark_df_meningitis_viral = spark.createDataFrame(df_meningitis_viral)
spark_df_meningitis_bacteriana = spark.createDataFrame(df_meningitis_bacteriana)
spark_df_leptospirosis = spark.createDataFrame(df_leptospirosis)
spark_df_dengue = spark.createDataFrame(df_dengue)

# Registrar los DataFrames como tablas temporales en Spark
spark_df_meningitis_viral.createOrReplaceTempView("meningitis_viral")
spark_df_meningitis_bacteriana.createOrReplaceTempView("meningitis_bacteriana")
spark_df_leptospirosis.createOrReplaceTempView("leptospirosis")
spark_df_dengue.createOrReplaceTempView("dengue")

# Mostrar las primeras filas de cada tabla en Spark
print("Tabla Spark - Meningitis Viral:")
spark.sql("SELECT * FROM meningitis_viral").show()

print("Tabla Spark - Meningitis Bacteriana:")
spark.sql("SELECT * FROM meningitis_bacteriana").show()

print("Tabla Spark - Leptospirosis:")
spark.sql("SELECT * FROM leptospirosis").show()

print("Tabla Spark - Dengue:")
spark.sql("SELECT * FROM dengue").show()


# COMMAND ----------

# MAGIC %md
# MAGIC Envio los mensajes a cada topic de Kafka

# COMMAND ----------

import json
import time
import random
import pandas as pd
from confluent_kafka import Producer

# COMMAND ----------

from confluent_kafka import Producer
import json
import random
import time


producer = Producer(conf)

# Definir los nombres de los tópicos
topics = {
    "meningitis_viral": "notif_mening_viral",
    "meningitis_bacteriana": "notif_mening_bact",
    "leptospirosis": "notif_leptospirosis",
    "dengue": "notif_dengue"
}

# Crear una función para enviar mensajes a Kafka
def send_to_kafka(df, topic):
    # Convertir DataFrame a una lista de filas
    rows = df.collect()

    for i, row in enumerate(rows):
        key = str(i + 1)  # Puedes usar el índice o algún otro campo como key
        value = row.asDict()  # Convertir la fila a diccionario

        # Producir mensaje a Kafka
        producer.produce(topic=topic, key=key, value=json.dumps(value))
        producer.flush()

        print(f"Mensaje {i + 1} enviado al tópico {topic}: {value}")

        # Tiempo de espera aleatorio entre mensajes
        sleep_time = random.uniform(0, 1)
        print(f"Durmiendo por {sleep_time:.2f} segundos")
        time.sleep(sleep_time)

# Enviar los mensajes para cada DataFrame
send_to_kafka(spark_df_leptospirosis, topics["leptospirosis"])
send_to_kafka(spark_df_dengue, topics["dengue"])
send_to_kafka(spark_df_meningitis_viral, topics["meningitis_viral"])
send_to_kafka(spark_df_meningitis_bacteriana, topics["meningitis_bacteriana"])

