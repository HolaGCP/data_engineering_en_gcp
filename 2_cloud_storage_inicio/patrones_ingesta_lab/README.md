# Laboratorio de Patrones de ingesta

## Antes de Empezar

1. Para empezar primero abrir la terminal de cloud shell.

2. Clonar el repositorio en el directorio home.

```bash
cd &&rm -rf data_engineering_en_gcp && git clone https://github.com/HolaGCP/data_engineering_en_gcp.git
```

3. Abrir Cloud Shell Editor.

```bash
cloudshell ws $HOME/data_engineering_en_gcp
```
## Ejecución de scripts

1.- Crear el ambiente e instalar las dependencias.

```bash
cd 2_cloud_storage_inicio/patrones_ingesta_lab/
python3 -m venv .venv
source .venv/bin/activate && pip install --upgrade pip
pip install -r requirements.txt 
```

2.- Ejecutar el script del patrón de ingesta descarga de archivos

```bash
python descarga_archivos.py
```

3.- Ejecutar el script del patrón de ingesta parseo de textos

```bash
python parseo_textos.py
```