import requests
from storage import Storage


def main():
    cloud = Storage()
    bucket = cloud.crear_bucket(cloud.client.project)
    url = "https://radiorsd.pe/sites/default/files/2022-03/DESAPARECIDOS%20ANCASH%20.jpg"
    r = requests.get(url)
    nombre_archivo = "desaparecidos.jpg"
    archivo_destino = "../recursos/{}".format(nombre_archivo)
    with open(archivo_destino, 'wb') as f:
        f.write(r.content)
    cloud.subir_archivo(bucket, nombre_archivo, archivo_destino)


if __name__ == "__main__":
    main()
