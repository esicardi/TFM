# Databricks notebook source
jdbcHostname = "sql-eass-2024.database.windows.net"
jdbcPort = 1433
jdbcDatabase = "sql-eass"
jdbcUsername = "esicardi@sql-eass-2024"
jdbcPassword = "Pregunta42_"

# COMMAND ----------

# URL de conexión JDBC
jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase};user={jdbcUsername};password={jdbcPassword};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=500;"

# Propiedades de conexión
connectionProperties = {
  "user" : jdbcUsername,
  "password" : jdbcPassword,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Consulta a la base de datos
query = "(SELECT * FROM [dbo].[lab-data] WHERE estado = 'finalizado') as resultados_finalizados"

# Leer datos desde la base de datos a un DataFrame
df = spark.read.jdbc(url=jdbcUrl, table=query, properties=connectionProperties)

# Mostrar los datos
df.show()

# COMMAND ----------

from pymongo import MongoClient
import pandas as pd

# URL de conexión MongoDB
mongo_url = "mongodb+srv://myAtlasDBUser:Pregunta42_@myatlasclusteredu.x8divu1.mongodb.net/?retryWrites=true&w=majority&tls=true"

# Nombre de la base de datos y la colección
database_name = "vigilancia"
collection_name = "notificaciones_eventos"

# Conectar a MongoDB
client = MongoClient(mongo_url)

# Seleccionar la base de datos
db = client[database_name]

# Seleccionar la colección
collection = db[collection_name]

# Consultar los documentos en la colección
resultados = collection.find({"evento": "Dengue"})

# Convertir los resultados a un DataFrame de Pandas
df_mongo = pd.DataFrame(list(resultados))

# Mostrar los primeros registros del DataFrame
df_mongo.head()


# COMMAND ----------

# Contar el número de documentos en la colección
num_documents = collection.count_documents({})
print(f"Número total de documentos en la colección: {num_documents}")

# Mostrar algunos documentos para verificar su estructura
resultados = collection.find().limit(5)
for doc in resultados:
    print(doc)


# COMMAND ----------

# Verificar si el campo "evento" existe en los documentos y obtener ejemplos
resultados = collection.find({"evento": {"$exists": True}}).limit(100)
for doc in resultados:
    print(doc["evento"])


# COMMAND ----------

# Buscar documentos donde el campo "evento" contenga "Menigitis"
resultados = collection.find({"evento": {"$regex": "Meningitis", "$options": "i"}})

# Convertir los resultados a un DataFrame de Pandas
df_mongo = pd.DataFrame(list(resultados))

# Mostrar los primeros registros del DataFrame
df_mongo.head()


# COMMAND ----------

# Buscar documentos donde el campo "evento" contenga "Menigitis"
resultados = collection.find({"evento": "Meningitis Viral"})

# Convertir los resultados a un DataFrame de Pandas
df_mongo = pd.DataFrame(list(resultados))

# Mostrar los primeros registros del DataFrame
df_mongo.head()

# COMMAND ----------

from pymongo import MongoClient
from datetime import datetime, timedelta

# URL de conexión MongoDB
mongo_url = "mongodb+srv://myAtlasDBUser:Pregunta42_@myatlasclusteredu.x8divu1.mongodb.net/?retryWrites=true&w=majority&tls=true"

# Conectar a MongoDB
client = MongoClient(mongo_url)

# Seleccionar la base de datos y las colecciones
db = client["vigilancia"]
notificaciones_collection = db["notificaciones_eventos"]
casos_collection = db["casos_seguimiento"]

# Obtener todas las notificaciones
notificaciones = notificaciones_collection.find()

# Iterar sobre cada notificación
for notificacion in notificaciones:
    cedula = notificacion["cedula"]
    evento = notificacion["evento"]
    fecha_inicio_sintomas = notificacion["fecha_inicio_sintomas"]
    
    # Verificar si la fecha es una cadena y convertirla si es necesario
    if isinstance(fecha_inicio_sintomas, str):
        fecha_inicio_sintomas = datetime.strptime(fecha_inicio_sintomas, "%Y-%m-%d")
    
    # Buscar documentos en la colección "casos" con la misma cédula, evento y fecha_inicio_sintomas dentro de 30 días
    query = {
        "cedula": cedula,
        "evento": evento,
        "fecha_inicio_sintomas": {
            "$gte": fecha_inicio_sintomas - timedelta(days=30),
            "$lte": fecha_inicio_sintomas + timedelta(days=30)
        }
    }
    casos_existentes = casos_collection.count_documents(query)
    
    # Si no se encuentran casos existentes, crear un nuevo documento en "casos"
    if casos_existentes == 0:
        nuevo_documento = notificacion.copy()  # Copiar los campos del documento de notificaciones
        nuevo_documento["estado"] = "notificado"  # Añadir el campo "estado"
        
        # Insertar el nuevo documento en la colección "casos"
        casos_collection.insert_one(nuevo_documento)

print("Proceso completado. Nuevos documentos insertados en la colección 'casos_epi'.")



# COMMAND ----------

from pymongo import MongoClient
from datetime import datetime, timedelta

# URL de conexión MongoDB
mongo_url = "mongodb+srv://myAtlasDBUser:Pregunta42_@myatlasclusteredu.x8divu1.mongodb.net/?retryWrites=true&w=majority&tls=true"

# Conectar a MongoDB
client = MongoClient(mongo_url)

# Seleccionar la base de datos y la colección
db = client["vigilancia"]
casos_collection = db["casos_seguimiento"]

# Definir las reglas de AE
def calcular_ae(fecha):
    if datetime(2018, 1, 1) <= fecha <= datetime(2018, 12, 29):
        return 2018
    elif datetime(2018, 12, 30) <= fecha <= datetime(2019, 12, 28):
        return 2019
    elif datetime(2019, 12, 29) <= fecha <= datetime(2021, 1, 2):
        return 2020
    elif datetime(2021, 1, 3) <= fecha <= datetime(2022, 1, 1):
        return 2021
    elif datetime(2022, 1, 2) <= fecha <= datetime(2022, 12, 31):
        return 2022
    elif datetime(2023, 1, 1) <= fecha <= datetime(2023, 12, 30):
        return 2023
    elif datetime(2023, 12, 31) <= fecha <= datetime(2024, 12, 28):
        return 2024
    else:
        return None

# Calcular la SE (Semana Epidemiológica)
def calcular_se(fecha, ae):
    # Definir los inicios de AE
    ae_inicios = {
        2018: datetime(2018, 1, 1),
        2019: datetime(2018, 12, 30),
        2020: datetime(2019, 12, 29),
        2021: datetime(2021, 1, 3),
        2022: datetime(2022, 1, 2),
        2023: datetime(2023, 1, 1),
        2024: datetime(2023, 12, 31),
    }
    
    inicio_ae = ae_inicios.get(ae)
    
    if inicio_ae:
        delta = fecha - inicio_ae
        return (delta.days // 7) + 1  # Contar semanas a partir del inicio del AE
    else:
        return None

# Iterar sobre los documentos en la colección "casos" y actualizar AE y SE
for caso in casos_collection.find():
    fecha_inicio_sintomas = caso.get("fecha_inicio_sintomas")
    
    if isinstance(fecha_inicio_sintomas, str):
        fecha_inicio_sintomas = datetime.strptime(fecha_inicio_sintomas, "%Y-%m-%d")
    
    ae = calcular_ae(fecha_inicio_sintomas)
    se = calcular_se(fecha_inicio_sintomas, ae)
    
    if ae is not None and se is not None:
        # Actualizar el documento con los nuevos campos AE y SE
        casos_collection.update_one(
            {"_id": caso["_id"]},
            {"$set": {"AE": ae, "SE": se}}
        )

print("Proceso completado. Los documentos en la colección 'casos_epi' han sido actualizados con AE y SE.")


# COMMAND ----------

from pymongo import MongoClient
import datetime
from pyspark.sql import SparkSession

# Configurar los detalles de la conexión JDBC
jdbcHostname = "sql-eass-2024.database.windows.net"
jdbcPort = 1433
jdbcDatabase = "sql-eass"
jdbcUsername = "esicardi@sql-eass-2024"
jdbcPassword = "Pregunta42_"
jdbcUrl = (f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase};"
           f"user={jdbcUsername};password={jdbcPassword};"
           "encrypt=true;trustServerCertificate=false;"
           "hostNameInCertificate=*.database.windows.net;loginTimeout=30;")

# Crear la sesión de Spark
spark = SparkSession.builder.appName("Conexión a SQL Server desde Databricks").getOrCreate()

# Conectar a MongoDB
mongo_url = "mongodb+srv://myAtlasDBUser:Pregunta42_@myatlasclusteredu.x8divu1.mongodb.net/?retryWrites=true&w=majority&tls=true"
client = MongoClient(mongo_url)
db = client["vigilancia"]

# Asegurarse de que las colecciones existan
casos_collection = db.get_collection("casos_seguimiento")
alarmas_collection = db.get_collection("alertas_epi")

# Leer los datos desde la tabla dbo.resultados en la base de datos SQL
query = "(SELECT * FROM [dbo].[lab-data] WHERE estado = 'finalizado') as finalizados"
df_sql = spark.read.jdbc(url=jdbcUrl, table=query)

# Mostrar los primeros registros del DataFrame SQL
display(df_sql)

# Función para convertir `datetime.date` a `datetime.datetime`
def to_datetime(value):
    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
        return datetime.datetime.combine(value, datetime.datetime.min.time())
    return value

# Iterar sobre los documentos en la colección "casos"
casos = casos_collection.find({"estado": {"$in": ["notificado", "seguimiento"]}})

for caso in casos:
    cedula = caso["cedula"]
    evento = caso["evento"]
    
    # Filtrar el DataFrame SQL para buscar la cédula y el evento específicos
    sql_filtrado = df_sql.filter((df_sql["cedula"] == cedula) & (df_sql["evento"] == evento))
    
    if sql_filtrado.count() == 0:
        # No hay registros en SQL para esta cédula, generar una alarma
        nueva_alarma = {
            "fecha": datetime.datetime.now(),
            "cedula": cedula,
            "alarma": "no tiene muestra en laboratorio",
            "nombre": caso["nombre"],
            "apellido": caso["apellido"],
            "departamento": caso["departamento"],
            "evento": evento,
            "prestador": caso["prestador"],
            "fecha_inicio_sintomas": to_datetime(caso["fecha_inicio_sintomas"])
        }
        # Insertar la nueva alarma en MongoDB
        alarmas_collection.insert_one(nueva_alarma)
    else:
        # Hay registros en SQL, actualizar el documento en la colección "casos"
        resultados_laboratorio = sql_filtrado.collect()
        
        # Determinar el nuevo estado basado en los resultados
        nuevo_estado = "seguimiento"
        for resultado in resultados_laboratorio:
            if resultado.estado.lower() == "finalizado":
                if resultado.resultado.lower() == "positivo":
                    nuevo_estado = "confirmado"
                elif resultado.resultado.lower() == "negativo":
                    nuevo_estado = "descartado"
                else:
                    nuevo_estado = "seguimiento"
            else:
                nuevo_estado = "seguimiento"
        
        # Asegurarse de que todas las fechas estén en formato datetime
        caso_actualizado = {
            "estado": nuevo_estado,
            "resultados_laboratorio": [
                {k: to_datetime(v) for k, v in row.asDict().items()} for row in resultados_laboratorio
            ]
        }
        
        # Actualizar el documento en "casos"
        casos_collection.update_one(
            {"_id": caso["_id"]},
            {"$set": caso_actualizado}
        )


# COMMAND ----------

from pymongo import MongoClient
from pyspark.sql import SparkSession

# Configurar los detalles de la conexión JDBC
jdbcHostname = "sql-eass-2024.database.windows.net"
jdbcPort = 1433
jdbcDatabase = "sql-eass"
jdbcUsername = "esicardi@sql-eass-2024"
jdbcPassword = "Pregunta42_"
jdbcUrl = (f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase};"
           f"user={jdbcUsername};password={jdbcPassword};"
           "encrypt=true;trustServerCertificate=false;"
           "hostNameInCertificate=*.database.windows.net;loginTimeout=500;")

# Crear la sesión de Spark
spark = SparkSession.builder.appName("Conexión a SQL Server desde Databricks").getOrCreate()

# Conectar a MongoDB
mongo_url = "mongodb+srv://myAtlasDBUser:Pregunta42_@myatlasclusteredu.x8divu1.mongodb.net/?retryWrites=true&w=majority&tls=true"
client = MongoClient(mongo_url)
db = client["vigilancia"]

# Asegurarse de que las colecciones existan
casos_collection = db.get_collection("casos_seguimiento")

# Leer los datos desde la tabla dbo.resultados en la base de datos SQL
df_sql = spark.read.jdbc(url=jdbcUrl, table="[dbo].[lab-data]")

# Obtener las cédulas de la base SQL
cedulas_sql = df_sql.select("cedula").distinct().rdd.flatMap(lambda x: x).collect()

# Obtener las cédulas de la colección "casos" en MongoDB
cedulas_mongo = casos_collection.distinct("cedula")

# Convertir las listas de cédulas a conjuntos para facilitar la intersección
set_cedulas_sql = set(cedulas_sql)
set_cedulas_mongo = set(cedulas_mongo)

# Encontrar cédulas que están en ambas bases de datos
cedulas_repetidas = set_cedulas_sql.intersection(set_cedulas_mongo)

# Contar cuántas cédulas se repiten
cantidad_repetidas = len(cedulas_repetidas)

print(f"Cantidad de cédulas que se repiten en ambas bases de datos: {cantidad_repetidas}")


# COMMAND ----------

from pymongo import MongoClient
from pyspark.sql import SparkSession

# Configurar los detalles de la conexión JDBC
jdbcHostname = "sql-eass.database.windows.net"
jdbcPort = 1433
jdbcDatabase = "laboratorio"
jdbcUsername = "esicardi@sql-eass"
jdbcPassword = "Pregunta42_"
jdbcUrl = (f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase};"
           f"user={jdbcUsername};password={jdbcPassword};"
           "encrypt=true;trustServerCertificate=false;"
           "hostNameInCertificate=*.database.windows.net;loginTimeout=500;")

# Crear la sesión de Spark
spark = SparkSession.builder.appName("Conexión a SQL Server desde Databricks").getOrCreate()

# Conectar a MongoDB
mongo_url = "mongodb+srv://myAtlasDBUser:Pregunta42_@myatlasclusteredu.x8divu1.mongodb.net/?retryWrites=true&w=majority&tls=true"
client = MongoClient(mongo_url)
db = client["vigilancia"]

# Asegurarse de que las colecciones existan
casos_collection = db.get_collection("casos_epi")

# Leer los datos desde la tabla dbo.resultados en la base de datos SQL
df_sql = spark.read.jdbc(url=jdbcUrl, table="dbo.resultados")

# Obtener las cédulas de la base SQL
cedulas_sql = df_sql.select("cedula").distinct().rdd.flatMap(lambda x: x).collect()

# Obtener las cédulas de la colección "casos" en MongoDB
cedulas_mongo = casos_collection.distinct("cedula")

# Convertir las listas de cédulas a conjuntos para facilitar la intersección
set_cedulas_sql = set(cedulas_sql)
set_cedulas_mongo = set(cedulas_mongo)

# Encontrar cédulas que están en ambas bases de datos
cedulas_repetidas = set_cedulas_sql.intersection(set_cedulas_mongo)

# Mostrar las cédulas repetidas
print("Cédulas presentes en ambas bases de datos:")
for cedula in cedulas_repetidas:
    print(cedula)


# COMMAND ----------

print(cedulas_sql)

# COMMAND ----------

print(cedulas_mongo)

# COMMAND ----------

from pymongo import MongoClient
from pyspark.sql import SparkSession

# Configurar los detalles de la conexión JDBC
jdbcHostname = "sql-eass-2024.database.windows.net"
jdbcPort = 1433
jdbcDatabase = "sql-eass"
jdbcUsername = "esicardi@sql-eass-2024"
jdbcPassword = "Pregunta42_"
jdbcUrl = (f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase};"
           f"user={jdbcUsername};password={jdbcPassword};"
           "encrypt=true;trustServerCertificate=false;"
           "hostNameInCertificate=*.database.windows.net;loginTimeout=30;")

# Crear la sesión de Spark
spark = SparkSession.builder.appName("Conexión a SQL Server desde Databricks").getOrCreate()

# Conectar a MongoDB
mongo_url = "mongodb+srv://myAtlasDBUser:Pregunta42_@myatlasclusteredu.x8divu1.mongodb.net/?retryWrites=true&w=majority&tls=true"
client = MongoClient(mongo_url)
db = client["vigilancia"]

# Asegurarse de que las colecciones existan
casos_collection = db.get_collection("casos_seguimiento")

# Leer los datos desde la tabla dbo.resultados en la base de datos SQL
df_sql = spark.read.jdbc(url=jdbcUrl, table="[dbo].[lab-data]")

# Obtener las cédulas de la base SQL
cedulas_sql = df_sql.select("cedula").distinct().rdd.flatMap(lambda x: x).map(int).collect()

# Obtener las cédulas de la colección "casos" en MongoDB y convertirlas a enteros
cedulas_mongo = casos_collection.distinct("cedula")
cedulas_mongo = [int(cedula) for cedula in cedulas_mongo if cedula.isdigit()]

# Convertir las listas de cédulas a conjuntos para facilitar la intersección
set_cedulas_sql = set(cedulas_sql)
set_cedulas_mongo = set(cedulas_mongo)

# Encontrar cédulas que están en ambas bases de datos
cedulas_repetidas = set_cedulas_sql.intersection(set_cedulas_mongo)

# Mostrar las cédulas repetidas
print("Cédulas presentes en ambas bases de datos:")
for cedula in cedulas_repetidas:
    print(cedula)


# COMMAND ----------

tamaño_cedulas_repetidas = len(cedulas_repetidas)
print(f"Tamaño de cedulas_repetidas: {tamaño_cedulas_repetidas}")


# COMMAND ----------

from pymongo import MongoClient
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Configurar los detalles de la conexión JDBC
jdbcHostname = "sql-eass-2024.database.windows.net"
jdbcPort = 1433
jdbcDatabase = "sql-eass"
jdbcUsername = "esicardi@sql-eass-2024"
jdbcPassword = "Pregunta42_"
jdbcUrl = (f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase};"
           f"user={jdbcUsername};password={jdbcPassword};"
           "encrypt=true;trustServerCertificate=false;"
           "hostNameInCertificate=*.database.windows.net;loginTimeout=30;")

# Crear la sesión de Spark
spark = SparkSession.builder.appName("Conexión a SQL Server desde Databricks").getOrCreate()

# Conectar a MongoDB
mongo_url = "mongodb+srv://myAtlasDBUser:Pregunta42_@myatlasclusteredu.x8divu1.mongodb.net/?retryWrites=true&w=majority&tls=true"
client = MongoClient(mongo_url)
db = client["vigilancia"]

# Asegurarse de que las colecciones existan
casos_collection = db.get_collection("casos_seguimiento")
alarmas_collection = db.get_collection("alertas_epidemiologicas")

# Leer los datos desde la tabla dbo.resultados en la base de datos SQL
query = "(SELECT * FROM [dbo].[lab-data] WHERE estado = 'finalizado') as finalizados"
df_sql = spark.read.jdbc(url=jdbcUrl, table=query)

# Convertir la columna "cedula" en df_sql a entero
df_sql = df_sql.withColumn("cedula", col("cedula").cast("int"))

# Mostrar los primeros registros del DataFrame SQL
display(df_sql)

# Función para convertir `datetime.date` a `datetime.datetime`
def to_datetime(value):
    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
        return datetime.datetime.combine(value, datetime.datetime.min.time())
    return value

# Iterar sobre los documentos en la colección "casos"
casos = casos_collection.find({"estado": {"$in": ["notificado", "seguimiento"]}})

for caso in casos:
    # Convertir la cédula de MongoDB a entero para comparar
    cedula = int(caso["cedula"])
    evento = caso["evento"]
    
    # Filtrar el DataFrame SQL para buscar la cédula y el evento específicos
    sql_filtrado = df_sql.filter((col("cedula") == cedula) & (col("evento") == evento))
    
    if sql_filtrado.count() == 0:
        # No hay registros en SQL para esta cédula, generar una alarma
        nueva_alarma = {
            "fecha": datetime.datetime.now(),
            "cedula": cedula,
            "alarma": "no tiene muestra en laboratorio",
            "nombre": caso["nombre"],
            "apellido": caso["apellido"],
            "departamento": caso["departamento"],
            "evento": evento,
            "prestador": caso["prestador"],
            "fecha_inicio_sintomas": to_datetime(caso["fecha_inicio_sintomas"])
        }
        # Insertar la nueva alarma en MongoDB
        alarmas_collection.insert_one(nueva_alarma)
    else:
        # Hay registros en SQL, actualizar el documento en la colección "casos"
        resultados_laboratorio = sql_filtrado.collect()
        
        # Determinar el nuevo estado basado en los resultados
        nuevo_estado = "seguimiento"
        for resultado in resultados_laboratorio:
            if resultado.estado.lower() == "finalizado":
                if resultado.resultado.lower() == "positivo":
                    nuevo_estado = "confirmado"
                elif resultado.resultado.lower() == "negativo":
                    nuevo_estado = "descartado"
                else:
                    nuevo_estado = "seguimiento"
            else:
                nuevo_estado = "seguimiento"
        
        # Asegurarse de que todas las fechas estén en formato datetime
        caso_actualizado = {
            "estado": nuevo_estado,
            "resultados_laboratorio": [
                {k: to_datetime(v) for k, v in row.asDict().items()} for row in resultados_laboratorio
            ]
        }
        
        # Actualizar el documento en "casos"
        casos_collection.update_one(
            {"_id": caso["_id"]},
            {"$set": caso_actualizado}
        )


# COMMAND ----------

# Obtener los posibles valores únicos de la columna 'evento' en SQL Server
valores_evento_sql = df_sql.select("evento").distinct().collect()
valores_evento_sql = [row["evento"] for row in valores_evento_sql]

print("Valores únicos de 'evento' en SQL Server:")
print(valores_evento_sql)


# COMMAND ----------

# Obtener los posibles valores únicos de la columna 'evento' en MongoDB
pipeline = [
    {"$match": {"estado": {"$in": ["notificado", "seguimiento"]}}},
    {"$group": {"_id": "$evento"}}
]
valores_evento_mongo = list(casos_collection.aggregate(pipeline))
valores_evento_mongo = [doc["_id"] for doc in valores_evento_mongo]

print("Valores únicos de 'evento' en MongoDB:")
print(valores_evento_mongo)


# COMMAND ----------

from pymongo import MongoClient
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Configurar los detalles de la conexión JDBC
jdbcHostname = "sql-eass-2024.database.windows.net"
jdbcPort = 1433
jdbcDatabase = "sql-eass"
jdbcUsername = "esicardi@sql-eass-2024"
jdbcPassword = "Pregunta42_"
jdbcUrl = (f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase};"
           f"user={jdbcUsername};password={jdbcPassword};"
           "encrypt=true;trustServerCertificate=false;"
           "hostNameInCertificate=*.database.windows.net;loginTimeout=30;")

# Crear la sesión de Spark
spark = SparkSession.builder.appName("Conexión a SQL Server desde Databricks").getOrCreate()

# Conectar a MongoDB
mongo_url = "mongodb+srv://myAtlasDBUser:Pregunta42_@myatlasclusteredu.x8divu1.mongodb.net/?retryWrites=true&w=majority&tls=true"
client = MongoClient(mongo_url)
db = client["vigilancia"]

# Asegurarse de que las colecciones existan
casos_collection = db.get_collection("casos_seguimiento")
alarmas_collection = db.get_collection("alarmas_epidemiologicas")

# Leer los datos desde la tabla dbo.resultados en la base de datos SQL
query = "(SELECT * FROM [dbo].[lab-data] WHERE estado = 'finalizado') as finalizados"
df_sql = spark.read.jdbc(url=jdbcUrl, table=query)

# Convertir la columna "cedula" en df_sql a entero
df_sql = df_sql.withColumn("cedula", col("cedula").cast("int"))

# Mostrar los primeros registros del DataFrame SQL
display(df_sql)

# Función para convertir `datetime.date` a `datetime.datetime`
def to_datetime(value):
    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
        return datetime.datetime.combine(value, datetime.datetime.min.time())
    return value

# Iterar sobre los documentos en la colección "casos"
casos = casos_collection.find({"estado": {"$in": ["notificado", "seguimiento"]}})

for caso in casos:
    # Convertir la cédula de MongoDB a entero para comparar
    cedula = int(caso["cedula"])
    evento = caso["evento"]
    
    # Filtrar el DataFrame SQL para buscar la cédula y el evento específicos
    sql_filtrado = df_sql.filter((col("cedula") == cedula))
    
    if sql_filtrado.count() == 0:
        # No hay registros en SQL para esta cédula, generar una alarma
        nueva_alarma = {
            "fecha": datetime.datetime.now(),
            "cedula": cedula,
            "alarma": "no tiene muestra en laboratorio",
            "nombre": caso["nombre"],
            "apellido": caso["apellido"],
            "departamento": caso["departamento"],
            "evento": evento,
            "prestador": caso["prestador"],
            "fecha_inicio_sintomas": to_datetime(caso["fecha_inicio_sintomas"])
        }
        # Insertar la nueva alarma en MongoDB
        alarmas_collection.insert_one(nueva_alarma)
    else:
        # Hay registros en SQL, actualizar el documento en la colección "casos"
        resultados_laboratorio = sql_filtrado.collect()
        
        # Determinar el nuevo estado basado en los resultados
        nuevo_estado = "seguimiento"
        for resultado in resultados_laboratorio:
            if resultado.estado.lower() == "finalizado":
                if resultado.resultado.lower() == "positivo":
                    nuevo_estado = "confirmado"
                elif resultado.resultado.lower() == "negativo":
                    nuevo_estado = "descartado"
                else:
                    nuevo_estado = "seguimiento"
            else:
                nuevo_estado = "seguimiento"
        
        # Asegurarse de que todas las fechas estén en formato datetime
        caso_actualizado = {
            "estado": nuevo_estado,
            "resultados_laboratorio": [
                {k: to_datetime(v) for k, v in row.asDict().items()} for row in resultados_laboratorio
            ]
        }
        
        # Actualizar el documento en "casos"
        casos_collection.update_one(
            {"_id": caso["_id"]},
            {"$set": caso_actualizado}
        )


# COMMAND ----------

from pymongo import MongoClient

# Conectar a MongoDB
mongo_url = "mongodb+srv://myAtlasDBUser:Pregunta42_@myatlasclusteredu.x8divu1.mongodb.net/?retryWrites=true&w=majority&tls=true"
client = MongoClient(mongo_url)
db = client["vigilancia"]

# Asegurarse de que la colección exista
casos_collection = db.get_collection("casos_seguimiento")

# Contar documentos que tienen el campo 'resultados_laboratorio'
count_resultados_laboratorio = casos_collection.count_documents({"resultados_laboratorio": {"$exists": True}})

print(f"Cantidad de documentos con el campo 'resultados_laboratorio': {count_resultados_laboratorio}")


# COMMAND ----------

from pymongo import MongoClient

# Conectar a MongoDB
mongo_url = "mongodb+srv://myAtlasDBUser:Pregunta42_@myatlasclusteredu.x8divu1.mongodb.net/?retryWrites=true&w=majority&tls=true"
client = MongoClient(mongo_url)
db = client["vigilancia"]

# Asegurarse de que la colección exista
casos_collection = db.get_collection("casos_seguimiento")

# Contar documentos que tienen el estado 'seguimiento'
count_seguimiento = casos_collection.count_documents({"estado": "descartado"})

print(f"Cantidad de documentos con el estado 'seguimiento': {count_seguimiento}")


# COMMAND ----------

from pymongo import MongoClient
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Configurar los detalles de la conexión JDBC
jdbcHostname = "sql-eass-2024.database.windows.net"
jdbcPort = 1433
jdbcDatabase = "sql-eass"
jdbcUsername = "esicardi@sql-eass-2024"
jdbcPassword = "Pregunta42_"
jdbcUrl = (f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase};"
           f"user={jdbcUsername};password={jdbcPassword};"
           "encrypt=true;trustServerCertificate=false;"
           "hostNameInCertificate=*.database.windows.net;loginTimeout=30;")

# Crear la sesión de Spark
spark = SparkSession.builder.appName("Conexión a SQL Server desde Databricks").getOrCreate()

# Conectar a MongoDB
mongo_url = "mongodb+srv://myAtlasDBUser:Pregunta42_@myatlasclusteredu.x8divu1.mongodb.net/?retryWrites=true&w=majority&tls=true"
client = MongoClient(mongo_url)
db = client["vigilancia"]

# Asegurarse de que las colecciones existan
casos_collection = db.get_collection("casos-seguimientos")
alarmas_collection = db.get_collection("alarmas-epidemiologicas")

# Leer los datos desde la tabla dbo.resultados en la base de datos SQL
query = "(SELECT * FROM [dbo].[lab-data]) as resultados"
df_sql = spark.read.jdbc(url=jdbcUrl, table=query)

# Convertir la columna "cedula" en df_sql a entero
df_sql = df_sql.withColumn("cedula", col("cedula").cast("int"))

# Mostrar los primeros registros del DataFrame SQL
display(df_sql)

# Función para convertir `datetime.date` a `datetime.datetime`
def to_datetime(value):
    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
        return datetime.datetime.combine(value, datetime.datetime.min.time())
    return value

# Iterar sobre los documentos en la colección "casos"
casos = casos_collection.find({"estado": {"$in": ["notificado", "seguimiento"]}})

for caso in casos:
    # Convertir la cédula de MongoDB a entero para comparar
    cedula = int(caso["cedula"])
    evento = caso["evento"]
    
    # Filtrar el DataFrame SQL para buscar la cédula
    sql_filtrado = df_sql.filter(col("cedula") == cedula)
    
    # Comprobar si hay registros para esta cédula
    if sql_filtrado.count() == 0:
        # No hay registros en SQL para esta cédula, generar una alarma
        nueva_alarma = {
            "fecha": datetime.datetime.now(),
            "cedula": cedula,
            "alarma": "no tiene muestra en laboratorio",
            "nombre": caso["nombre"],
            "apellido": caso["apellido"],
            "departamento": caso["departamento"],
            "evento": evento,
            "prestador": caso["prestador"],
            "fecha_inicio_sintomas": to_datetime(caso["fecha_inicio_sintomas"])
        }
        # Insertar la nueva alarma en MongoDB
        alarmas_collection.insert_one(nueva_alarma)
    else:
        # Hay registros en SQL, actualizar el documento en la colección "casos"
        resultados_laboratorio = sql_filtrado.collect()
        
        # Determinar el nuevo estado basado en los resultados
        nuevo_estado = "seguimiento"
        alarmas = []
        for resultado in resultados_laboratorio:
            if resultado.estado.lower() == "finalizado":
                if resultado.resultado.lower() == "positivo":
                    nuevo_estado = "confirmado"
                    alarmas.append("investigacion de campo")
                elif resultado.resultado.lower() == "negativo":
                    nuevo_estado = "descartado"
                elif resultado.resultado.lower() == "indeterminado":
                    nuevo_estado = "seguimiento"
                    alarmas.append("se solicita segunda muestra")
            else:
                nuevo_estado = "seguimiento"
        
        # Asegurarse de que todas las fechas estén en formato datetime
        caso_actualizado = {
            "estado": nuevo_estado,
            "resultados_laboratorio": [
                {k: to_datetime(v) for k, v in row.asDict().items()} for row in resultados_laboratorio
            ]
        }
        
        # Actualizar el documento en "casos"
        casos_collection.update_one(
            {"_id": caso["_id"]},
            {"$set": caso_actualizado}
        )
        
        # Insertar alarmas relacionadas
        for alarma in alarmas:
            nueva_alarma = {
                "fecha": datetime.datetime.now(),
                "cedula": cedula,
                "alarma": alarma,
                "nombre": caso["nombre"],
                "apellido": caso["apellido"],
                "departamento": caso["departamento"],
                "evento": evento,
                "prestador": caso["prestador"],
                "fecha_inicio_sintomas": to_datetime(caso["fecha_inicio_sintomas"])
            }
            # Insertar la nueva alarma en MongoDB
            alarmas_collection.insert_one(nueva_alarma)


# COMMAND ----------

from pymongo import MongoClient
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Configurar los detalles de la conexión JDBC
jdbcHostname = "sql-eass-2024.database.windows.net"
jdbcPort = 1433
jdbcDatabase = "sql-eass"
jdbcUsername = "esicardi@sql-eass-2024"
jdbcPassword = "Pregunta42_"
jdbcUrl = (f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase};"
           f"user={jdbcUsername};password={jdbcPassword};"
           "encrypt=true;trustServerCertificate=false;"
           "hostNameInCertificate=*.database.windows.net;loginTimeout=30;")

# Crear la sesión de Spark
spark = SparkSession.builder.appName("Conexión a SQL Server desde Databricks").getOrCreate()

# Conectar a MongoDB
mongo_url = "mongodb+srv://myAtlasDBUser:Pregunta42_@myatlasclusteredu.x8divu1.mongodb.net/?retryWrites=true&w=majority&tls=true"
client = MongoClient(mongo_url)
db = client["vigilancia"]

# Asegurarse de que las colecciones existan
casos_collection = db.get_collection("casos_seguimiento")
alarmas_collection = db.get_collection("alarmas_epidemiologicos")

# Leer los datos desde la tabla dbo.resultados en la base de datos SQL
query = "(SELECT * FROM [dbo].[lab-data]) as resultados"
df_sql = spark.read.jdbc(url=jdbcUrl, table=query)

# Convertir la columna "cedula" en df_sql a entero
df_sql = df_sql.withColumn("cedula", col("cedula").cast("int"))

# Mostrar los primeros registros del DataFrame SQL
display(df_sql)

# Función para convertir `datetime.date` a `datetime.datetime`
def to_datetime(value):
    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
        return datetime.datetime.combine(value, datetime.datetime.min.time())
    return value

# Iterar sobre los documentos en la colección "casos"
casos = casos_collection.find({"estado": {"$in": ["notificado", "seguimiento"]}})

for caso in casos:
    # Convertir la cédula de MongoDB a entero para comparar
    cedula = int(caso["cedula"])
    evento = caso["evento"]
    
    # Filtrar el DataFrame SQL para buscar la cédula
    sql_filtrado = df_sql.filter(col("cedula") == cedula)
    
    # Comprobar si hay registros para esta cédula
    if sql_filtrado.count() == 0:
        # No hay registros en SQL para esta cédula, generar una alarma
        nueva_alarma = {
            "fecha": datetime.datetime.now(),
            "cedula": cedula,
            "alarma": "no tiene muestra en laboratorio",
            "nombre": caso["nombre"],
            "apellido": caso["apellido"],
            "departamento": caso["departamento"],
            "evento": evento,
            "prestador": caso["prestador"],
            "fecha_inicio_sintomas": to_datetime(caso["fecha_inicio_sintomas"])
        }
        # Insertar la nueva alarma en MongoDB
        alarmas_collection.insert_one(nueva_alarma)
    else:
        # Hay registros en SQL, actualizar el documento en la colección "casos"
        resultados_laboratorio = sql_filtrado.collect()
        
        # Determinar el nuevo estado basado en los resultados
        nuevo_estado = "seguimiento"
        alarmas = []
        for resultado in resultados_laboratorio:
            if resultado.estado.lower() == "finalizado":
                if resultado.resultado.lower() == "positivo":
                    nuevo_estado = "confirmado"
                    alarmas.append("investigacion de campo")
                elif resultado.resultado.lower() == "negativo":
                    nuevo_estado = "descartado"
                elif resultado.resultado.lower() == "indeterminado":
                    nuevo_estado = "seguimiento"
                    alarmas.append("se solicita segunda muestra")
        
        # Asegurarse de que todas las fechas estén en formato datetime
        caso_actualizado = {
            "estado": nuevo_estado,
            "resultados_laboratorio": [
                {k: to_datetime(v) for k, v in row.asDict().items()} for row in resultados_laboratorio
            ]
        }
        
        # Actualizar el documento en "casos"
        casos_collection.update_one(
            {"_id": caso["_id"]},
            {"$set": caso_actualizado}
        )
        
        # Insertar alarmas relacionadas
        if alarmas:
            for alarma in alarmas:
                nueva_alarma = {
                    "fecha": datetime.datetime.now(),
                    "cedula": cedula,
                    "alarma": alarma,
                    "nombre": caso["nombre"],
                    "apellido": caso["apellido"],
                    "departamento": caso["departamento"],
                    "evento": evento,
                    "prestador": caso["prestador"],
                    "fecha_inicio_sintomas": to_datetime(caso["fecha_inicio_sintomas"])
                }
                # Insertar la nueva alarma en MongoDB
                alarmas_collection.insert_one(nueva_alarma)


# COMMAND ----------

from pymongo import MongoClient
import datetime

# Conectar a MongoDB
mongo_url = "mongodb+srv://myAtlasDBUser:Pregunta42_@myatlasclusteredu.x8divu1.mongodb.net/?retryWrites=true&w=majority&tls=true"
client = MongoClient(mongo_url)
db = client["vigilancia"]

# Asegurarse de que las colecciones existan
casos_collection = db.get_collection("casos_seguimiento")
alertas_collection = db.get_collection("alarmas_epidemiologicas")

# Iterar sobre los documentos en la colección "casos"
casos = casos_collection.find({"estado": "confirmado"})

for caso in casos:
    cedula = caso.get("cedula")
    evento = caso.get("evento")
    
    # Crear el nuevo documento de alerta
    nueva_alerta = {
        "fecha": datetime.datetime.now(),
        "cedula": cedula,
        "alarma": "investigacion de campo",
        "nombre": caso.get("nombre"),
        "apellido": caso.get("apellido"),
        "departamento": caso.get("departamento"),
        "evento": evento,
        "prestador": caso.get("prestador"),
        "fecha_inicio_sintomas": caso.get("fecha_inicio_sintomas")
    }
    
    # Insertar la nueva alerta en la colección "alertas"
    alertas_collection.insert_one(nueva_alerta)

print("Alarmas de investigación de campo generadas exitosamente.")


# COMMAND ----------

from pyspark.sql.functions import col
# Cargar los documentos de la colección "casos" en un DataFrame de Spark
casos_df = spark.read.format("mongo").option("uri", "mongodb+srv://myAtlasDBUser:Pregunta42_@myatlasclusteredu.x8divu1.mongodb.net/?retryWrites=true&w=majority&tls=true").option("database", "vigilancia").option("collection", "casos_seguimiento").load()

# Filtrar los documentos por estado 'confirmado' y por año epidemiológico 2024
casos_df_filtered = casos_df.filter((col("estado") == "confirmado") & (col("AE") == 2024))

# Contar los documentos confirmados por evento y semana epidemiológica (SE)
result_df = casos_df_filtered.groupBy("evento", "SE").count()

# Generar una lista completa de combinaciones de evento y SE (de 1 a 33)
eventos = casos_df_filtered.select("evento").distinct().rdd.flatMap(lambda x: x).collect()
semanas = list(range(1, 34))

# COMMAND ----------

result_df.printSchema()


# COMMAND ----------

# Define los parámetros de configuración
account = "stmd01estrella"
key = "aO0SDuNHT7RQFaCaMN+YiW2R5829YhVT2GtOey+Utaw611i/J+b300eZMYSAfVbNeMz8U/X94aVC+AStgeMROw=="
container_name = "silver"
silver_path = f"abfss://{container_name}@{account}.dfs.core.windows.net"

# Configura la clave de acceso para el almacenamiento
spark.conf.set(f"fs.azure.account.key.{account}.dfs.core.windows.net", key)
# Configuración para guardar en el Delta Lake en la capa Silver

spark.sql("DROP TABLE IF EXISTS conteo_confirmados_2024")
result_df.write.format("delta").mode("overwrite").save(silver_path + "/conteo_confirmados_2024")
result_df.write.format("delta").mode("overwrite").saveAsTable("conteo_confirmados_2024")

# COMMAND ----------

spark.sql("DESCRIBE TABLE conteo_confirmados_2024").show()


# COMMAND ----------

spark.sql("SELECT COUNT(*) FROM conteo_confirmados_2024").show()


# COMMAND ----------

spark.sql("SELECT * FROM conteo_confirmados_2024 LIMIT 132").display()


# COMMAND ----------

# Contar registros en el DataFrame original
df_count = result_df.count()

# Contar registros en la tabla escrita
table_count = spark.sql("SELECT COUNT(*) FROM conteo_confirmados_2024").collect()[0][0]

# Comparar conteos
print(f"Registros en el DataFrame original: {df_count}")
print(f"Registros en la tabla 'conteo_confirmados_2024': {table_count}")

# Mostrar una muestra de datos de la tabla
spark.sql("SELECT * FROM conteo_confirmados_2024 LIMIT 10").show()


# COMMAND ----------

dbutils.fs.ls(silver_path + "/conteo_confirmados_2024")


# COMMAND ----------

# Listar los archivos en la ruta del Delta Lake
display(dbutils.fs.ls(silver_path + "/conteo_confirmados_2024"))


# COMMAND ----------

 # Leer datos desde Delta Lake
delta_df = spark.read.format("delta").load(silver_path + "/conteo_confirmados_2024")

# Mostrar algunas filas
delta_df.show(10)


# COMMAND ----------

# Contar registros en el Delta Lake
delta_count = delta_df.count()
print(f"Número de registros en Delta Lake: {delta_count}")


# COMMAND ----------

# Verificar existencia de la tabla
tables = spark.catalog.listTables()
if any(table.name == "conteo_confirmados_2024" for table in tables):
    print("La tabla 'conteo_confirmados_2024' existe en el catálogo de Spark SQL.")
else:
    print("La tabla 'conteo_confirmados_2024' no existe en el catálogo de Spark SQL.")


# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

spark.createDataFrame([token.as_dict() for token in w.token_management.list()]).createOrReplaceTempView('tokens')

display(spark.sql('select * from tokens order by creation_time'))

# COMMAND ----------

from databricks.sdk import WorkspaceClient

# Retrieve Databricks workspace host and token from environment
dbutils = DatabricksUtilities.get_instance()
host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

# Initialize the WorkspaceClient with explicit host and token
w = WorkspaceClient(host=host, token=token)

# Your existing code to create DataFrame and display it
spark.createDataFrame([token.as_dict() for token in w.token_management.list()]).createOrReplaceTempView('tokens')
display(spark.sql('select * from tokens order by creation_time'))

# COMMAND ----------

# Retrieve Databricks workspace host and token from environment
host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

# Initialize the WorkspaceClient with explicit host and token
w = WorkspaceClient(host=host, token=token)

# Your existing code to create DataFrame and display it
spark.createDataFrame([token.as_dict() for token in w.token_management.list()]).createOrReplaceTempView('tokens')
display(spark.sql('select * from tokens order by creation_time'))

# COMMAND ----------

from pyspark.sql import SparkSession

# Suponiendo que ya tienes una sesión Spark activa
spark = SparkSession.builder.getOrCreate()

# Verificar si la tabla existe
tables = spark.catalog.listTables()
if any(table.name == "conteo_confirmados_2024" for table in tables):
    print("La tabla 'conteo_confirmados_2024' existe en el catálogo de Spark SQL.")
    
    # Leer la tabla en un DataFrame
    df = spark.table("conteo_confirmados_2024")
    
    # Ruta en DBFS
    dbfs_path = "/dbfs/tmp/conteo_confirmados_2024.csv"
    
    # Guardar el DataFrame en un archivo CSV en DBFS
    df.write.csv(dbfs_path, header=True, mode="overwrite")
    print(f"Datos guardados en el archivo CSV en {dbfs_path}.")
else:
    print("La tabla 'conteo_confirmados_2024' no existe en el catálogo de Spark SQL.")


# COMMAND ----------

from pyspark.sql import SparkSession

# Crear una sesión Spark
spark = SparkSession.builder.getOrCreate()

# Verificar si la tabla existe
tables = spark.catalog.listTables()
if any(table.name == "conteo_confirmados_2024" for table in tables):
    print("La tabla 'conteo_confirmados_2024' existe en el catálogo de Spark SQL.")
    
    # Leer la tabla en un DataFrame
    df = spark.table("conteo_confirmados_2024")
    
    # Ruta en DBFS
    dbfs_path = "/dbfs/Workspace/Users/esicardi@gmail.com/conteo_confirmados_2024.csv"
    
    # Guardar el DataFrame en un archivo CSV en DBFS
    df.write.csv(dbfs_path, header=True, mode="overwrite")
    print(f"Datos guardados en el archivo CSV en {dbfs_path}.")
else:
    print("La tabla 'conteo_confirmados_2024' no existe en el catálogo de Spark SQL.")


# COMMAND ----------

from pyspark.sql import SparkSession

# Crear una sesión Spark
spark = SparkSession.builder.getOrCreate()

# Verificar si la tabla existe
tables = spark.catalog.listTables()
if any(table.name == "conteo_confirmados_2024" for table in tables):
    print("La tabla 'conteo_confirmados_2024' existe en el catálogo de Spark SQL.")
    
    # Leer la tabla en un DataFrame
    df = spark.table("conteo_confirmados_2024")
    
    # Ruta en DBFS para guardar el archivo CSV
    dbfs_path = "/dbfs/Workspace/Users/esicardi@gmail.com/conteo_confirmados_2024.csv"
    
    # Guardar el DataFrame en un archivo CSV en DBFS
    df.write.csv(path=dbfs_path, header=True, mode="overwrite", sep=",")
    print(f"Datos guardados en el archivo CSV en {dbfs_path}.")
else:
    print("La tabla 'conteo_confirmados_2024' no existe en el catálogo de Spark SQL.")


# COMMAND ----------

import pandas as pd

# Verificar si la tabla existe
tables = spark.catalog.listTables()
if any(table.name == "conteo_confirmados_2024" for table in tables):
    print("La tabla 'conteo_confirmados_2024' existe en el catálogo de Spark SQL.")
    
    # Leer la tabla en un DataFrame de Spark
    df_spark = spark.table("conteo_confirmados_2024")
    
    # Convertir a DataFrame de Pandas
    df_pandas = df_spark.toPandas()
    
    # Especificar la ruta del archivo en el Workspace
    path_in_workspace = "/Workspace/Users/esicardi@gmail.com/conteo_confirmados_2024.csv"
    
    # Guardar como un CSV tradicional usando Pandas
    df_pandas.to_csv(path_in_workspace, index=False)
    print(f"Archivo CSV guardado en el Workspace en {path_in_workspace}.")
else:
    print("La tabla 'conteo_confirmados_2024' no existe en el catálogo de Spark SQL.")


# COMMAND ----------

import pandas as pd
import os

# Verificar si la tabla existe
tables = spark.catalog.listTables()
if any(table.name == "conteo_confirmados_2024" for table in tables):
    print("La tabla 'conteo_confirmados_2024' existe en el catálogo de Spark SQL.")
    
    # Leer la tabla en un DataFrame de Spark
    df_spark = spark.table("conteo_confirmados_2024")
    
    # Convertir a DataFrame de Pandas
    df_pandas = df_spark.toPandas()
    
    # Especificar la ruta del archivo en el Workspace
    path_in_workspace = "/Workspace/Users/esicardi@gmail.com/conteo_confirmados_2024.csv"
    
    # Verificar si el directorio existe, si no, crearlo
    directory = os.path.dirname(path_in_workspace)
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    # Guardar como un CSV tradicional usando Pandas
    df_pandas.to_csv(path_in_workspace, index=False)
    print(f"Archivo CSV guardado en el Workspace en {path_in_workspace}.")
else:
    print("La tabla 'conteo_confirmados_2024' no existe en el catálogo de Spark SQL.")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

# Crear una lista de todas las combinaciones de evento y SE
eventos_se = [(evento, se) for evento in eventos for se in semanas]
schema = ["evento", "SE"]
eventos_se_df = spark.createDataFrame(eventos_se, schema=schema)

# Contar total de casos, confirmados y descartados
total_casos_df = casos_df_filtered.groupBy("evento", "SE").count().withColumnRenamed("count", "total_casos")

total_confirmados_df = casos_df_filtered.filter(col("estado") == "confirmado") \
    .groupBy("evento", "SE").count().withColumnRenamed("count", "total_confirmados")

total_descartados_df = casos_df_filtered.filter(col("estado") == "descartado") \
    .groupBy("evento", "SE").count().withColumnRenamed("count", "total_descartados")

# Unir todas las agregaciones
curva_epi = eventos_se_df \
    .join(total_casos_df, ["evento", "SE"], "left") \
    .join(total_confirmados_df, ["evento", "SE"], "left") \
    .join(total_descartados_df, ["evento", "SE"], "left") \
    .na.fill(0)  # Rellenar valores nulos con 0

# Ordenar por evento y SE
curva_epi = curva_epi.orderBy("evento", "SE")

# Mostrar el resultado
curva_epi.show(100)


# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col 

# Cargar los documentos de la colección "casos" en un DataFrame de Spark
casos_df = spark.read.format("mongo").option("uri", "mongodb+srv://myAtlasDBUser:Pregunta42_@myatlasclusteredu.x8divu1.mongodb.net/?retryWrites=true&w=majority&tls=true").option("database", "vigilancia").option("collection", "casos").load()

# Filtrar los documentos por año epidemiológico 2024
casos_df_filtered = casos_df.filter(col("AE") == 2024)

# Obtener lista de eventos únicos
eventos = casos_df_filtered.select("evento").distinct().rdd.flatMap(lambda x: x).collect()
semanas = list(range(1, 34))

# Crear un DataFrame con todas las combinaciones de evento y SE
eventos_se = [(evento, se) for evento in eventos for se in semanas]
schema = ["evento", "SE"]
eventos_se_df = spark.createDataFrame(eventos_se, schema=schema)

# Contar el total de casos por evento y SE
total_casos_df = casos_df_filtered.groupBy("evento", "SE").count().withColumnRenamed("count", "total_casos")

# Contar el total de confirmados por evento y SE
total_confirmados_df = casos_df_filtered.filter(col("estado") == "confirmado") \
    .groupBy("evento", "SE").count().withColumnRenamed("count", "total_confirmados")

# Contar el total de descartados por evento y SE
total_descartados_df = casos_df_filtered.filter(col("estado") == "descartado") \
    .groupBy("evento", "SE").count().withColumnRenamed("count", "total_descartados")

# Unir los resultados con todas las combinaciones posibles de evento y SE
curva_epi = eventos_se_df \
    .join(total_casos_df, ["evento", "SE"], "left") \
    .join(total_confirmados_df, ["evento", "SE"], "left") \
    .join(total_descartados_df, ["evento", "SE"], "left") \
    .na.fill(0)  # Rellenar valores nulos con 0

# Ordenar por evento y SE
curva_epi = curva_epi.orderBy("evento", "SE")

# Mostrar el resultado final
curva_epi.show(100)


# COMMAND ----------

curva_epi.show(600)


# COMMAND ----------

from pyspark.sql import SparkSession

# Define los parámetros de configuración
account = "stmd01estrella"
key = "aO0SDuNHT7RQFaCaMN+YiW2R5829YhVT2GtOey+Utaw611i/J+b300eZMYSAfVbNeMz8U/X94aVC+AStgeMROw=="
container_name = "silver"
silver_path = f"abfss://{container_name}@{account}.dfs.core.windows.net/curva_epi"

# Configura Spark session para acceder a Azure Data Lake Storage Gen2
spark.conf.set(f"fs.azure.account.key.{account}.dfs.core.windows.net", key)

# Guardar el DataFrame 'curva_epi' en formato Delta
curva_epi.write.format("delta").mode("overwrite").save(silver_path)


# COMMAND ----------

from pyspark.sql import SparkSession

# Crear una Spark session (si no está ya creada)
spark = SparkSession.builder.appName("Leer Delta Table").getOrCreate()

# Configura el acceso a Azure Data Lake Storage Gen2
account = "stmd01estrella"
key = "aO0SDuNHT7RQFaCaMN+YiW2R5829YhVT2GtOey+Utaw611i/J+b300eZMYSAfVbNeMz8U/X94aVC+AStgeMROw=="
container_name = "silver"
silver_path = f"abfss://{container_name}@{account}.dfs.core.windows.net/curva_epi"

# Configura Spark para acceder al almacenamiento de Azure Data Lake
spark.conf.set(f"fs.azure.account.key.{account}.dfs.core.windows.net", key)

# Leer la tabla guardada en formato Delta
curva_epi_df = spark.read.format("delta").load(silver_path)

# Mostrar los primeros registros
curva_epi_df.show()


# COMMAND ----------

from pyspark.sql import SparkSession

# Crear una Spark session (si no está ya creada)
spark = SparkSession.builder.appName("Grabar Delta Table en nuevo ADLS").getOrCreate()

# Configura los nuevos parámetros de almacenamiento
account = "dls01eass"
key = "3JWVYNCiKq8gkrkAy0CmF6TWriiwpCT95Oxb8SyBG4COigK59ByU3fDOCUKED6zFZd9cjGwVe5Bk+AStJ6N+vA=="
container_name = "silver"
silver_path = f"abfss://{container_name}@{account}.dfs.core.windows.net/curva_epi"

# Configura Spark session para acceder a Azure Data Lake Storage Gen2
spark.conf.set(f"fs.azure.account.key.{account}.dfs.core.windows.net", key)

# Guardar el DataFrame 'curva_epi' en formato Delta en la nueva ubicación
curva_epi_df.write.format("delta").mode("overwrite").save(silver_path)

print(f"Archivo guardado exitosamente en {silver_path}")


# COMMAND ----------


