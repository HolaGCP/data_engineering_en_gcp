# Creando dags en Composer

1. Óbten la ruta de cloud storage del folder de dags.

```bash
dags_folder=$(gcloud composer environments describe composer \
    --location us-east1 \
    --format="get(config.dagGcsPrefix)")
```
2. Copia el dag simple a cloud storage y espera a que composer lo parsee.

```bash
gsutil cp 6_composer/dags/simple.py $dags_folder/
```

3. Copia el dag bigquery a cloud storage y espera a que composer lo parsee.

```bash
gsutil cp 6_composer/dags/simple.py $dags_folder/
```