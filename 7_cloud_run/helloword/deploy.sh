
# Metodo 1: Construir image via cloud build
# Envio de la imagen a Cloud Build para que construya la imagen
gcloud builds submit --tag us-east1-docker.pkg.dev/secret-footing-366022/cloud-run-source-deploy/helloword:latest .
# Despligue de la imagen en Cloud Run
gcloud run deploy helloword-from-cloud-build --image us-east1-docker.pkg.dev/secret-footing-366022/cloud-run-source-deploy/helloword:latest \
--region us-east1 --allow-unauthenticated


# Metodo 2: Construir imagen localmente

PROJECT_ID=$(gcloud config get-value project)
REPO_NAME=cloud-run-source-deploy
IMAGE_NAME=helloword
docker image build -t us-east1-docker.pkg.dev/${PROJECT_ID}/$REPO_NAME/$IMAGE_NAME\:latest .

gcloud auth configure-docker -q

docker push us-east1-docker.pkg.dev/${PROJECT_ID}/$REPO_NAME/$IMAGE_NAME\:latest

gcloud run deploy helloword-from-cloud-build2 --image us-east1-docker.pkg.dev/${PROJECT_ID}/$REPO_NAME/$IMAGE_NAME\:latest \
--region us-east1 --allow-unauthenticated