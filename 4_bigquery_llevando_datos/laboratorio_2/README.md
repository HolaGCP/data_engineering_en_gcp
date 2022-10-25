# Laboratorio para construir las 3 capas del data warehouse.

En laboratorio vamos a utilizar la base de datos muestra de microsoft [AdventureWorks](https://learn.microsoft.com/en-us/sql/samples/adventureworks-install-configure?view=sql-server-ver16&tabs=ssms).

Pues encontrar información y modelos ER en el siguiente [link](https://dataedo.com/samples/html/AdventureWorks/doc/AdventureWorks_2/modules.html)

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

## Laboratorio

1. Ejecutar script para capa raw [shell script](1_creando_capa_raw.sh).

2. Ejecutar script para capa staging [query](2_creando_capa_staging.sql).

3. Ejecutar script para capa analytics [query](3_creando_capa_analytics.sql).
