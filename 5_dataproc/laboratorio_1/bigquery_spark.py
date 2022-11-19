#!/usr/bin/env python
# coding: utf-8

# # 1.1. BigQuery Storage & Spark DataFrames - Python

# ### Crea un cluster de Dataproc con Jupyter
# 
# Este notebook esta diseñado para ser ejecutado en Google Cloud Dataproc.
# Siga este tutorial para crear el clúster de Dataproc.
# 
# * [Tutorial - Instalar y ejecutar un notebook de jupyter en un cluster de Dataproc](https://cloud.google.com/dataproc/docs/tutorials/jupyter-notebook)

# ### Python 3 Kernel
# 
# Use un kernel de Python 3 (no PySpark) para permitirle configurar SparkSession en el notebook e incluir el [conector de spark-bigquery-connector](https://github.com/GoogleCloudDataproc/spark-bigquery-connector) requerido para usar el [API de Bigquery Storage](https://cloud.google.com/bigquery/docs/reference/storage).

# ### Create Spark Session

# In[7]:


from pyspark.sql import SparkSession
spark = SparkSession.builder   .appName('1.1. BigQuery Storage & Spark DataFrames - Python')  .config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar')   .getOrCreate()


# ### Habilita repl.eagerEval
# 
# Esto generará los resultados de DataFrames en cada paso sin la nueva necesidad de mostrar `df.show ()` y también mejora el formato de la salida

# In[8]:


if hasattr(__builtins__,'__IPYTHON__'):
    spark.conf.set("spark.sql.repl.eagerEval.enabled",True)


# ### Lea tabla de BigQuery en Spark DataFrame
# 
# Usa `filter()` para consultar datos de una tabla particionada.

# In[9]:


table = "bigquery-public-data.wikipedia.pageviews_2022"
df_wiki_pageviews = spark.read   .format("bigquery")   .option("table", table)   .option("filter", "datehour >= '2022-10-01' AND datehour < '2022-10-02'")   .load()

df_wiki_pageviews.printSchema()


# Seleccione las columnas requeridas y aplique un filtro usando `where()` que es un alias para `filter()` y luego almacene en caché la tabla

# In[13]:


df_wiki_es = df_wiki_pageviews   .select("datehour","title", "wiki", "views")   .where("views > 1000 AND wiki in ('es', 'es.m')")   .cache()

df_wiki_es


# Agrupar por título y ordenar por vistas de página para ver las páginas principales

# In[12]:


import pyspark.sql.functions as F

df_wiki_en_totals = df_wiki_es .groupBy("title") .agg(F.sum('views').alias('total_views'))

df_wiki_en_totals.orderBy('total_views', ascending=False)


# ### Escriba Spark Dataframe to BigQuery table
# 
# Escriba el Spark Dataframe en la tabla de BigQuery mediante el conector de almacenamiento de BigQuery. Esto también creará la tabla si no existe.
# 
# Primero debemos crear o verificar el bucket y el dataset

# In[24]:


from google.cloud import storage
client = storage.Client()
bucket_name = client.project
try:
    client.get_bucket(bucket_name)
except:
    client.create_bucket(bucket_name, location="us-east1")
    print("Bucket {} creado".format(bucket_name))


# In[25]:


from google.cloud import bigquery

bq_client = bigquery.Client()
dataset_id = "{}.dataproc".format(bq_client.project)
try:
    bq_client.get_dataset(dataset_id)
except:
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "us-east1"
    dataset = bq_client.create_dataset(dataset)  # Make an API request.
    print("Dataset {}.{} creado".format(bq_client.project, dataset.dataset_id))


# In[27]:


# If the table does not exist it will be created when you run the write function
bq_table = 'wiki_total_pageviews'

df_wiki_en_totals.write   .format("bigquery")   .option("table","{}.{}".format(dataset_id, bq_table))   .option("temporaryGcsBucket", bucket_name)   .mode('overwrite')   .save()


# ### Usa "BigQuery magic" para consultar la tabla
# 
# Utilice [BigQuery magic](https://googleapis.dev/python/bigquery/latest/magics.html) para comprobar si los datos se crearon correctamente en BigQuery. Esto ejecutará la consulta SQL en BigQuery y devolverá los resultados.

# In[28]:


# get_ipython().run_cell_magic('bigquery', '', 'SELECT title, total_views\nFROM dataproc.wiki_total_pageviews\nORDER BY total_views DESC\nLIMIT 10')

