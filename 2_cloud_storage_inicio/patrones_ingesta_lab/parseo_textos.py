from bs4 import BeautifulSoup
import requests
from storage import Storage


def parse(url, destino):
    r = requests.get(url)
    web = r.text
    scraping = BeautifulSoup(web, "html.parser")
    tabla = scraping("table")[0]
    contenido = ""
    for fila in tabla("thead")[0]("tr")[0]:
        if fila.name == "th":
            contenido += "{},".format(fila.get_text())
    contenido = contenido[0:len(contenido) - 1]
    contenido += "\n"
    for x in tabla("tbody")[0]("tr"):
        for fila in x("td"):
            if len(fila.get_text()) > 0:
                contenido += "{},".format(fila.get_text())
        contenido = contenido[0:len(contenido) - 1]
        contenido += "\n"
    with open(destino, 'wb') as f:
        f.write(contenido.encode())


def main():
    cloud = Storage()
    bucket = cloud.crear_bucket(cloud.client.project)
    url = "https://gist.github.com/Karenprisi/0c042b809be666598562"
    archivo = "desaparecidos.csv"
    destino = "../recursos/{}".format(archivo)
    parse(url, destino)
    cloud.subir_archivo(bucket, archivo, destino)


if __name__ == "__main__":
    main()
