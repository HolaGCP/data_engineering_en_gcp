# Cargando Tablas de Binance a Bigquery

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

## Ejercicios

1. Abrir terminal del editor y verificar que el proyecto este configurado.

2. Crear un bucket con el mismo nombre del proyecto en caso no exista.

```bash
project=$(gcloud config get project)
gsutil mb -b on -l us-east1 gs://$project/
```

3. Descargar el archivo y cargarlo a cloud storage en el mismo bucket de proyecto.

```bash
curl -O "https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2022-07.zip"
unzip BTCUSDT-1m-2022-07.zip
gsutil cp BTCUSDT-1m-2022-07.csv gs://$project/binance-data/BTCUSDT-1m-2022-07.csv
```
4. Cargar la tabla desde la linea de comandos.

```bash
bq mk --dataset --location us-east1 binance
bq load --source_format=CSV --replace --allow_quoted_newlines binance.BTCUSDT gs://$project/binance-data/BTCUSDT-1m-2022-07.csv 3_bigquery_inicio/schema.json
```

5. Cargar la tabla con el transfer service.

6. Crear el transfer service con linea de comandos.
