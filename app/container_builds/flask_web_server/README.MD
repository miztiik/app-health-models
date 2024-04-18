

# Login to Azure Container Registry

```bash
export ACR_NAME="containeryard004"
export IMG_NAME="flask-web-server"

az login
az acr login --name ${ACR_NAME}
```
```

```bash
#!/bin/bash
docker build -t ${IMG_NAME} .
# docker run --rm ${IMG_NAME}
docker run -p 80:80 ${IMG_NAME}
```

## Tag the image

```bash
docker tag ${IMG_NAME} ${ACR_NAME}.azurecr.io/miztiik/${IMG_NAME}:v1
```

## Push the image to the registry

```bash
docker push ${ACR_NAME}.azurecr.io/miztiik/${IMG_NAME}:v1
```

## Run the image from the registry

```bash
# docker run -it --rm -p 8080:80 mcr.microsoft.com/oss/nginx/nginx:stable
docker run -it --rm -p 8080:80 ${ACR_NAME}.azurecr.io/miztiik/${IMG_NAME}

```

