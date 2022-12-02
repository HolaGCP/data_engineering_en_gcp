
# Laboratorio: Ejecutando Spark en GCP.

## Abrir Cloud Shell Editor

1. Para empezar primero abrir la terminal de cloud shell.

2. Clonar el repositorio en el directorio home.

```bash
cd &&rm -rf data_engineering_en_gcp && git clone https://github.com/HolaGCP/data_engineering_en_gcp.git
```

3. Abrir Cloud Shell Editor.

```bash
cloudshell ws $HOME/data_engineering_en_gcp
```

## Ejecutando jobs en un cluster instanciado.

1. Crea un cluster de Dataproc con Jupyter. [Tutorial](https://cloud.google.com/dataproc/docs/tutorials/jupyter-notebook)

2. [Ejecuta el notebook de spark con Cloud Storage](1_spark_cloud_storage.ipynb)

3. [Ejecuta el notebook de spark con Bigquery.](2_spark_bigquery.ipynb)

4. Ejecuta jobs en el cluster por línea de comandos.

```bash
(cd 5_dataproc/laboratorio_1/ &&
 gcloud dataproc jobs submit pyspark bigquery_spark.py \
    --cluster=cluster-fdd3 \
    --region=us-east1 \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar
)
```

## Ejecutando jobs en cluster efímeros.

1. Copia el código de pyspark a un bucket de gcp.

```bash
project=$(gcloud config get project)
(cd 5_dataproc/laboratorio_1/ &&
gsutil cp bigquery_spark.py gs://$project/dataproc/bigquery_spark.py
)
```

2. Crea el template de workflow.

```bash
gcloud dataproc workflow-templates create my-workflow \
    --region=us-east1
```

3. Agrega el cluster al template.

```bash
gcloud dataproc workflow-templates set-managed-cluster my-workflow \
    --region=us-east1 \
    --master-machine-type=n1-standard-4 \
    --worker-machine-type=n1-standard-4 \
    --num-workers=2 \
    --cluster-name=ephemeral-cluster
```

4. Agrega el job al template.

```bash
gcloud dataproc workflow-templates add-job pyspark gs://$project/dataproc/bigquery_spark.py \
    --region=us-east1 \
    --step-id=foo \
    --workflow-template=my-workflow \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar
```
5. Ejecuta el template.

```bash
gcloud dataproc workflow-templates instantiate my-workflow \
    --region=us-east1
```

6. Puedes exportar el workflow a un archivo yaml

```bash
(cd 5_dataproc/laboratorio_1/ &&
gcloud dataproc workflow-templates export my-workflow \
    --destination=template.yaml \
    --region=us-east1
)
```

7. E importarlo nuevamente.

```bash

(cd 5_dataproc/laboratorio_1/ &&
gcloud dataproc workflow-templates import my-workflow \
    --source=template.yaml \
    --region=us-east1
)
```

## Ejecutando jobs en spark serverless

1. Habilita el acceso privado a la red.

```bash

gcloud compute networks subnets update default \
--region=us-east1 \
--enable-private-ip-google-access
```

2. Ejecuta el job en spark serverless.

```bash
gcloud dataproc batches submit pyspark gs://$project/dataproc/bigquery_spark.py \
    --region=us-east1 \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    --subnet default
```