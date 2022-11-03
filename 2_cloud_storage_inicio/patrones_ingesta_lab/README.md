# Laboratorio de Patrones de ingesta

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