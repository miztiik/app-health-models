

# Login to Azure Container Registry



```bash
export ACR_NAME="containeryard004"
az acr login --name ${ACR_NAME}
```
```

```bash
#!/bin/bash
docker build -t echo-hello .
docker run --rm echo-hello
```

## Tag the image

```bash
docker tag echo-hello ${ACR_NAME}.azurecr.io/miztiik/echo-hello:v1
```

## Push the image to the registry

```bash
docker push ${ACR_NAME}.azurecr.io/miztiik/echo-hello:v1
```

## Run the image from the registry

```bash
# docker run -it --rm -p 8080:80 mcr.microsoft.com/oss/nginx/nginx:stable
docker run -it --rm -p 8080:80 ${ACR_NAME}.azurecr.io/miztiik/echo-hello

```

