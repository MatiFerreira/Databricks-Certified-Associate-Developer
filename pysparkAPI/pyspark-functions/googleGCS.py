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

# Descargar el jar necesario para conectarse al bucket de GCP

!wget https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.9/gcs-connector-hadoop3-2.2.9-shaded.jar

# Mover el jar descargado a la carpeta de jars de Spark

!mv gcs-connector-hadoop3-2.2.9-shaded.jar /content/spark-3.4.2-bin-hadoop3/jars

# Crear la sesión de Spark

import findspark

findspark.init()

from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .config("spark.hadoop.fs.gs.impl","com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
         .config("google.cloud.auth.service.account.json.keyfile","/content/pyspark.json")
         .getOrCreate())

df = spark.read.parquet('gs://josemtech/parquet')

df.show()

df.write.mode('overwrite').parquet('gs://josemtech/salida_parquet')

df1 = spark.read.option('header', 'true').csv('gs://josemtech/csv')

df1.show()

df1.write.mode('overwrite').csv('gs://josemtech/salida_csv')