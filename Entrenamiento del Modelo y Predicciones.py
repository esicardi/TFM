# Databricks notebook source
# MAGIC %md
# MAGIC Entrenamiento del Modelo

# COMMAND ----------

# Configura el acceso a Azure Data Lake Storage (ADLS)
account = "stmd01estrella"
key = "aO0SDuNHT7RQFaCaMN+YiW2R5829YhVT2GtOey+Utaw611i/J+b300eZMYSAfVbNeMz8U/X94aVC+AStgeMROw=="

spark.conf.set(f"fs.azure.account.key.{account}.dfs.core.windows.net", key)


# COMMAND ----------

# Ruta al archivo Excel en ADLS
excel_path = "abfss://miscelanea@stmd01estrella.dfs.core.windows.net/mi_archivo.xlsx"


# COMMAND ----------

# MAGIC %md
# MAGIC Cargo los datos de entrenamiento

# COMMAND ----------

from pyspark.sql import SparkSession

# Configura Spark
spark = SparkSession.builder \
    .appName("ExcelReader") \
    .getOrCreate()

# Ruta al archivo Excel en DBFS
dbfs_file_path = "dbfs:/Workspace/mi_archivo.xlsx"

# Lee el archivo Excel
df = spark.read.format("com.crealytics.spark.excel") \
    .option("dataAddress", "'Sheet1'!A1") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(dbfs_file_path)

# Muestra el contenido del DataFrame
df.show()


# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Entreno el modelo

# COMMAND ----------

import pandas as pd
import statsmodels.api as sm
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, confusion_matrix, roc_auc_score, roc_curve
import matplotlib.pyplot as plt
import mlflow
import mlflow.pyfunc
import numpy as np

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("LogisticRegressionExample") \
    .getOrCreate()

# Configurar el acceso a Azure Data Lake Storage (ADLS)
account = "stmd01estrella"
key = "aO0SDuNHT7RQFaCaMN+YiW2R5829YhVT2GtOey+Utaw611i/J+b300eZMYSAfVbNeMz8U/X94aVC+AStgeMROw=="
spark.conf.set(f"fs.azure.account.key.{account}.dfs.core.windows.net", key)

# Cargar el DataFrame de PySpark (ajusta según tu configuración)
# df = spark.read.csv("/ruta/a/tu/archivo.csv", header=True, inferSchema=True)

# Convertir el DataFrame de PySpark a pandas
pdf = df.toPandas()

# Definir las variables independientes y la variable dependiente
X = pdf[["dengue_TS", "dis_ciudad", "bio1_mean", "den_pob", "Alt_mean", "bio19_mean", "OrientNS", "bio15_mean"]]
y = pdf["dengue_SA"]

# Verificar si hay valores faltantes y manejarlos
print(X.isnull().sum())  # Muestra el número de valores faltantes por columna

# Imputar o eliminar valores faltantes si es necesario
X = X.fillna(0)  # Ejemplo de imputación con 0

# Añadir una constante a las variables independientes para incluir el intercepto en el modelo
X = sm.add_constant(X)

# Dividir los datos en conjunto de entrenamiento y prueba
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Configurar la ruta absoluta para el experimento en MLflow
experiment_path = "/Workspace/Users/estrellasicardi@udelar409.onmicrosoft.com/DengueLogReg7"
model_name = "DengueModel"

# Establecer el experimento en MLflow
mlflow.set_experiment(experiment_path)

# Finalizar cualquier ejecución activa previa
mlflow.end_run()
# Ruta del contenedor en ADLS
gold_container_path = f"abfss://gold@{account}.dfs.core.windows.net/Modelo_dengue/"
# Iniciar una nueva ejecución en MLflow
with mlflow.start_run() as run:
    # Ajustar el modelo de regresión logística
    logit_model = sm.Logit(y_train, X_train)
    result = logit_model.fit()

    # Mostrar el resumen del modelo
    print(result.summary())

    # Crear una clase que envuelva el modelo de statsmodels para usar con mlflow.pyfunc
    class StatsmodelsWrapper(mlflow.pyfunc.PythonModel):
        def __init__(self, model):
            self.model = model

        def predict(self, context, model_input):
            # Convertir entrada en un DataFrame
            input_df = pd.DataFrame(model_input, columns=self.model.exog_names)
            input_df = sm.add_constant(input_df)
            # Realizar predicciones
            return self.model.predict(input_df)

    # Registrar el modelo en MLflow
    signature = mlflow.models.infer_signature(X_train, y_train)
    mlflow.pyfunc.log_model("model", python_model=StatsmodelsWrapper(result), signature=signature)

    # Realizar predicciones
    y_pred_prob = result.predict(X_test)
    y_pred = [1 if x > 0.5 else 0 for x in y_pred_prob]  # Convertir probabilidades en predicciones binarias

    # Medir el rendimiento del modelo
    accuracy = accuracy_score(y_test, y_pred)
    conf_matrix = confusion_matrix(y_test, y_pred)
    roc_auc = roc_auc_score(y_test, y_pred_prob)

    # Registrar métricas en MLflow
    mlflow.log_metric("accuracy", accuracy)
    mlflow.log_metric("roc_auc", roc_auc)

    # Registrar la matriz de confusión como un artefacto
    with open("/tmp/confusion_matrix.txt", "w") as f:
        f.write(f"Confusion Matrix:\n{conf_matrix}")
    mlflow.log_artifact("/tmp/confusion_matrix.txt")

    # Graficar la curva ROC
    fpr, tpr, _ = roc_curve(y_test, y_pred_prob)
    plt.figure()
    plt.plot(fpr, tpr, color='blue', lw=2, label='ROC curve (area = %0.2f)' % roc_auc)
    plt.plot([0, 1], [0, 1], color='gray', lw=2, linestyle='--')
    plt.xlim([0.0, 1.0])
    plt.ylim([0.0, 1.05])
    plt.xlabel('False Positive Rate')
    plt.ylabel('True Positive Rate')
    plt.title('Receiver Operating Characteristic (ROC)')
    plt.legend(loc='lower right')
    plt.show()

    # Guardar los datos de entrenamiento en el contenedor gold con Spark y timestamp
    X_train_spark = spark.createDataFrame(X_train)
    X_train_spark = X_train_spark.withColumn("training_timestamp", current_timestamp())
    X_train_spark.write.format("delta").mode("overwrite").save(f"{gold_container_path}/datos_entrenamiento_delta")

    # Guardar los coeficientes del modelo en una tabla Spark
    coef_df = pd.DataFrame({
        'Variable': X_train.columns,
        'Coeficiente': result.params
    })
    coef_spark_df = spark.createDataFrame(coef_df)
    coef_spark_df.write.format("delta").mode("overwrite").save(f"{gold_container_path}/coeficientes_delta")

    # Imprimir información del run
    run_id = run.info.run_id
    print(f"Run ID: {run_id}")
    print(f"Accuracy: {accuracy:.4f}")
    print(f"Confusion Matrix:\n{conf_matrix}")
    print(f"ROC AUC Score: {roc_auc:.4f}")

    # Guardar en el Model Registry de Workspace
    mlflow.register_model(f"runs:/{run_id}/model", model_name)


# COMMAND ----------

import pickle

# Guardar los coeficientes en un archivo Pickle
coef_pickle_path = "/tmp/modelo_coeficientes.pkl"
with open(coef_pickle_path, "wb") as f:
    pickle.dump(result.params.to_dict(), f)

# Registrar el archivo Pickle como un artefacto en MLflow
mlflow.log_artifact(coef_pickle_path)


# COMMAND ----------

# MAGIC %md
# MAGIC Hago predicciones. Se calcula la performance nuevamente y se grafica curva ROC. Se observa que sigue teniendo un score parecido.

# COMMAND ----------

# Install openpyxl library
%pip install openpyxl


# COMMAND ----------

import pickle
import pandas as pd
import numpy as np
import statsmodels.api as sm

# Configurar la ruta del archivo Pickle
pickle_path = "/tmp/modelo_coeficientes.pkl"

# Cargar el modelo desde el archivo Pickle
with open(pickle_path, "rb") as f:
    model_params = pickle.load(f)

# Verificar el contenido de model_params
print("Modelo cargado:", model_params)

# Asegúrate de que model_params es un array NumPy, si es un diccionario, convierte sus valores en un array
if isinstance(model_params, dict):
    model_params = np.array(list(model_params.values()))

# Crear una función para realizar predicciones utilizando los coeficientes cargados
def predict_from_params(params, X):
    """
    Realiza predicciones usando los coeficientes del modelo.
    :param params: Coeficientes del modelo.
    :param X: Datos de entrada.
    :return: Predicciones del modelo.
    """
    X_array = X.values  # Convertir DataFrame a NumPy array
    X_array = sm.add_constant(X_array)  # Asegurarse de que la constante está incluida
    predictions = np.dot(X_array, params)  # Calcular las predicciones
    return predictions

# Leer datos desde el archivo Excel en Databricks
file_path = "/Workspace/Users/estrellasicardi@udelar409.onmicrosoft.com/datos_actualizados.xlsx"
excel_data = pd.read_excel(file_path)

# Preparar los datos para la predicción
data_for_prediction = excel_data[[
    "dengue_TS", "dis_ciudad", "bio1_mean", "den_pob", "Alt_mean", "bio19_mean", "OrientNS", "bio15_mean"
]].copy()  # Use copy to avoid SettingWithCopyWarning

# Realizar predicciones
predictions = predict_from_params(model_params, data_for_prediction)

# Convertir las predicciones a probabilidades usando la función sigmoide
probabilities = 1 / (1 + np.exp(-predictions))

# Convertir a clase binaria (asumiendo umbral de 0.5)
binary_predictions = [1 if p > 0.5 else 0 for p in probabilities]

# Mostrar las predicciones
print(binary_predictions)


# COMMAND ----------

import pickle
import pandas as pd
import numpy as np
import statsmodels.api as sm
from sklearn.metrics import accuracy_score, confusion_matrix, roc_auc_score, roc_curve
import matplotlib.pyplot as plt

# Configurar la ruta del archivo Pickle
pickle_path = "/tmp/modelo_coeficientes.pkl"

# Cargar el modelo desde el archivo Pickle
with open(pickle_path, "rb") as f:
    model_params = pickle.load(f)

# Verificar el contenido de model_params
print("Modelo cargado:", model_params)

# Asegúrate de que model_params es un array NumPy, si es un diccionario, convierte sus valores en un array
if isinstance(model_params, dict):
    model_params = np.array(list(model_params.values()))

# Crear una función para realizar predicciones utilizando los coeficientes cargados
def predict_from_params(params, X):
    """
    Realiza predicciones usando los coeficientes del modelo.
    :param params: Coeficientes del modelo.
    :param X: Datos de entrada.
    :return: Predicciones del modelo.
    """
    X_array = X.values  # Convertir DataFrame a NumPy array
    X_array = sm.add_constant(X_array)  # Asegurarse de que la constante está incluida
    predictions = np.dot(X_array, params)  # Calcular las predicciones
    return predictions

# Leer datos desde el archivo Excel en Databricks
file_path = "/Workspace/Users/estrellasicardi@udelar409.onmicrosoft.com/datos_actualizados.xlsx"
excel_data = pd.read_excel(file_path)

# Preparar los datos para la predicción
data_for_prediction = excel_data[[
    "dengue_TS", "dis_ciudad", "bio1_mean", "den_pob", "Alt_mean", "bio19_mean", "OrientNS", "bio15_mean"
]].copy()  # Use copy to avoid SettingWithCopyWarning

# Si tienes una columna de etiquetas reales (por ejemplo, 'dengue_SA') en el archivo, sepárala
# y asegúrate de que esté disponible para las métricas.
y_true = excel_data["dengue_SA"]  # Asegúrate de tener la etiqueta verdadera en el DataFrame

# Verificar y manejar NaNs en y_true y data_for_prediction
if y_true.isna().any():
    print("Advertencia: y_true contiene NaNs. Se eliminarán las filas con NaNs en y_true.")
    valid_indices = ~y_true.isna()
    y_true = y_true[valid_indices]
    data_for_prediction = data_for_prediction[valid_indices]

# Realizar predicciones
predictions = predict_from_params(model_params, data_for_prediction)

# Verificar y manejar NaNs en las predicciones
if np.isnan(predictions).any():
    print("Advertencia: Las predicciones contienen NaNs. Se reemplazarán por 0.")
    predictions = np.nan_to_num(predictions)

# Convertir las predicciones a probabilidades usando la función sigmoide
probabilities = 1 / (1 + np.exp(-predictions))

# Verificar y manejar NaNs en las probabilidades
if np.isnan(probabilities).any():
    print("Advertencia: Las probabilidades contienen NaNs. Se reemplazarán por 0.")
    probabilities = np.nan_to_num(probabilities)

# Convertir a clase binaria (asumiendo umbral de 0.5)
binary_predictions = [1 if p > 0.5 else 0 for p in probabilities]

# Medir el rendimiento del modelo
accuracy = accuracy_score(y_true, binary_predictions)
conf_matrix = confusion_matrix(y_true, binary_predictions)
roc_auc = roc_auc_score(y_true, probabilities)

# Registrar métricas en MLflow
print(f"Accuracy: {accuracy:.4f}")
print(f"Confusion Matrix:\n{conf_matrix}")
print(f"ROC AUC Score: {roc_auc:.4f}")

# Graficar la curva ROC
fpr, tpr, _ = roc_curve(y_true, probabilities)
plt.figure()
plt.plot(fpr, tpr, color='blue', lw=2, label='ROC curve (area = %0.2f)' % roc_auc)
plt.plot([0, 1], [0, 1], color='gray', lw=2, linestyle='--')
plt.xlim([0.0, 1.0])
plt.ylim([0.0, 1.05])
plt.xlabel('False Positive Rate')
plt.ylabel('True Positive Rate')
plt.title('Receiver Operating Characteristic (ROC)')
plt.legend(loc='lower right')
plt.show()

