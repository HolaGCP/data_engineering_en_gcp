from google.cloud import storage


class Storage:
    def __init__(self):
        self.client = storage.Client()

    def crear_bucket(self, bucket_name):
        try:
            bucket = self.client.create_bucket(bucket_name)
            print("Bucket {} creado.".format(bucket_name))
        except:
            bucket = self.client.get_bucket(bucket_name)
            print("Bucket {} ya existe.".format(bucket_name))
        return bucket

    def obtener_buckets(self):
        buckets = self.client.list_buckets()
        print("Buckets en {}:".format(self.client.project))
        for item in buckets:
            print("\t" + item.name)

    def obtener_datos_bucket(self, bucket):
        print("Bucket name: {}".format(bucket.name))
        print("Bucket location: {}".format(bucket.location))
        print("Bucket storage class: {}".format(bucket.storage_class))

    def subir_archivo(self, bucket, blob_name, source_file_name):
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(source_file_name)
        print("Archivo {} subido al bucket {}.".format(blob_name, bucket.name))

    def obtener_archivos(self, bucket):
        blobs = bucket.list_blobs()
        print("Archivos en {}:".format(bucket.name))
        for item in blobs:
            print("\t" + item.name)

    def obtener_datos_archivo(self, bucket, blob_name):
        blob = bucket.get_blob(blob_name)
        print("Name: {}".format(blob.id))
        print("Size: {} bytes".format(blob.size))
        print("Content type: {}".format(blob.content_type))
        print("Public URL: {}".format(blob.public_url))

    def descargar_archivo(self, bucket, blob_name, output_file_name):
        blob = bucket.get_blob(blob_name)
        blob.download_to_filename(output_file_name)
        print("Archivo {} descargado como {}.".format(blob.name, output_file_name))

    def eliminar_archivo(self, bucket, blob_name):
        blob = bucket.get_blob(blob_name)
        blob.delete()
        print("Archivo {} eliminado.".format(blob.name))

    def eliminar_bucket(self, bucket):
        bucket.delete()
        print("Bucket {} eliminado.".format(bucket.name))
