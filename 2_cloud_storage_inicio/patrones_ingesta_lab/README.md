1.- Instalar las dependencias con el comando

> pip install -r requirements.txt 

2.- Descargar las credenciales para acceder al proyecto en google cloud

> gcloud auth application-default login

3.- Declarar la variable de entorno GOOGLE_APPLICATION_CREDENTIALS apuntando a la ubicación del archivo json que contiene las credenciales

4.- Ejecutar el script del patrón de ingesta descarga de archivos

> python descarga_archivos.py

5.- Ejecutar el script del patrón de ingesta parseo de textos

> python parseo_textos.py
