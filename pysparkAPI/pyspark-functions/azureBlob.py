# Instalar SDK java 8

!apt-get install openjdk-8-jdk-headless -qq > /dev/null

# Descargar Spark

!wget -q https://archive.apache.org/dist/spark/spark-3.3.4/spark-3.3.4-bin-hadoop3.tgz

# Descomprimir la version de Spark

!tar xf spark-3.3.4-bin-hadoop3.tgz

# Establecer las variables de entorno

import os

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.3.4-bin-hadoop3"

# Descargar findspark

!pip install -q findspark

# Extraer las credenciales desde los Secrets

from google.colab import userdata

account_key = userdata.get('ACCOUNT_KEY')

# Crear la sesión de Spark

import findspark

findspark.init()

from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.6,com.microsoft.azure:azure-storage:8.6.6")
         .config("spark.hadoop.fs.azure.account.key.josemtech.blob.core.windows.net", account_key)
         .config("spark.hadoop.fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
         .config("spark.hadoop.fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
         .getOrCreate())

# Luctura

df = spark.read.parquet("wasbs://spark-data@josemtech.blob.core.windows.net/parquet")

df.show()

# Escritura

df.write.mode("overwrite").parquet("wasbs://spark-data@josemtech.blob.core.windows.net/test/")