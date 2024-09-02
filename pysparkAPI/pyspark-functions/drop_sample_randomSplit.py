# Transformaciones - funciones drop, sample y randomSplit

import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet('./data')

# drop

df.printSchema()

df_util = df.drop('comments_disabled')

df_util.printSchema()

df_util = df.drop('comments_disabled', 'ratings_disabled', 'thumbnail_link')

df_util.printSchema()

df_util = df.drop('comments_disabled', 'ratings_disabled', 'thumbnail_link', 'cafe')

df_util.printSchema()

# sample recoge de forma aleatoria un % de datos por ejemplo 0.8 recoge un 80% de datos aleatorios.

df_muestra = df.sample(0.8)

num_filas = df.count()
num_filas_muestra = df_muestra.count()

print('El 80% de filas del dataframe original es {}'.format(num_filas - (num_filas*0.2)))
print('El numero de filas del dataframe muestra es {}'.format(num_filas_muestra))

# EL SEED CUAL PODEMOS REPLICAR PARA PODER UTILIZARLO
df_muestra = df.sample(fraction=0.8, seed=1234)

# CON WITHREPLACEMENT REMPLAZO DE FILAS CON UNA FRACCION DE 0.8 CON LA SEMILLA 1234

df_muestra = df.sample(withReplacement=True, fraction=0.8, seed=1234)

# randomSplit MACHINE LEARNING MODEL

##LE PASAMOS  0.8 datos a entrenar y 0.2 donde se producira test usando la semilla posteriormente creada
train, test = df.randomSplit([0.8, 0.2], seed=1234)

train, validation, test = df.randomSplit([0.6, 0.2, 0.2], seed=1234)

train.count()

validation.count()

test.count()
