#!/bin/bash

# Docker Image Universe Destroyer
# docker rmi -f $(docker images -aq)

# Set Global Variables
ACR_NAME_PREFIX=$(jq -r '.parameters.acr_params.value.name_prefix' params.json)
GLOBAL_UNIQUENESS=$(jq -r '.parameters.deploymentParams.value.global_uniqueness' params.json)

CONTAINER_CODE_LOCATION="./app/container_builds/event_processor_for_svc_bus_queues/"

pushd ${CONTAINER_CODE_LOCATION}

export ACR_NAME="${ACR_NAME_PREFIX}${GLOBAL_UNIQUENESS}"
export IMG_NAME="event-processor-for-svc-bus-q"

# Login to Azure Container Registry
az acr login --name ${ACR_NAME}

## Build the image
docker build -t ${IMG_NAME} .

## Tag the image
BUILD_VERSION=$(date '+%Y-%m-%d-%H-%M')

docker tag ${IMG_NAME} ${ACR_NAME}.azurecr.io/miztiik/${IMG_NAME}
docker tag ${IMG_NAME} ${ACR_NAME}.azurecr.io/miztiik/${IMG_NAME}:${BUILD_VERSION}
docker tag ${IMG_NAME} ${ACR_NAME}.azurecr.io/miztiik/${IMG_NAME}:v1

## Push the image to the registry
docker push ${ACR_NAME}.azurecr.io/miztiik/${IMG_NAME}
docker push ${ACR_NAME}.azurecr.io/miztiik/${IMG_NAME}:${BUILD_VERSION}
docker push ${ACR_NAME}.azurecr.io/miztiik/${IMG_NAME}:v1


## Run the image from the registry

# docker run -it --rm -p 8080:80 mcr.microsoft.com/oss/nginx/nginx:stable
# docker run -it --rm -p 80:80 ${ACR_NAME}.azurecr.io/miztiik/${IMG_NAME}

# Return to home folder
popd