# Cargando Tablas de Binance a Bigquery


0. Este repo contiene alguno scripts que usamos durante el curso dado el 14/07/2022 al 18/07/2022.

```bash
cd
rm -rf data_engineering_en_gcp
git clone https://github.com/HolaGCP/data_engineering_en_gcp.git
cloudshell ws $HOME/data_engineering_en_gcp
```

De ser necesario setea el proyecto:

```bash
gcloud config set project nth-victory-357100
```

1. Descargar el archivo y cargarlo a cloud storage. antes de ejecutar el comando debes reemplazar el bucket de destino. En este laboratorio estamos usando "rimac-arnold-huete".

```bash
curl -O "https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2022-07.zip"
unzip BTCUSDT-1m-2022-07.zip
gsutil cp BTCUSDT-1m-2022-07.csv gs://rimac-arnold-huete/binance-data/BTCUSDT-1m-2022-07.csv
```

2. Cargar la tabla desde la consola de gcp.

3. Cargar la tabla desde la linea de comandos.

```bash
bq load --source_format=CSV --replace --allow_quoted_newlines binance.BTCUSDT gs://rimac-arnold-huete/binance-data/BTCUSDT-1m-2022-07.csv sesion03/schema.json
```

4. Cargar la tabla con el trasnfer service.

5. Crear el transfer service con linea de comandos.
