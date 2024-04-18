#!/bin/bash
# set -x
set -e

# Define colors for console output
GREEN="\e[32m"
RED="\e[31m"
BLUE="\e[34m"
CYAN="\e[36m"
YELLOW="\e[33m"
RESET="\e[0m"
# echo -e "${GREEN}This text is green.${RESET}"


# Set Global Variables
MAIN_BICEP_TEMPL_NAME="main.bicep"
LOCATION=$(jq -r '.parameters.deploymentParams.value.location' params.json)
SUB_DEPLOYMENT_PREFIX=$(jq -r '.parameters.deploymentParams.value.sub_deploymnet_prefix' params.json)
ENTERPRISE_NAME=$(jq -r '.parameters.deploymentParams.value.enterprise_name' params.json)
ENTERPRISE_NAME_SUFFIX=$(jq -r '.parameters.deploymentParams.value.enterprise_name_suffix' params.json)
LOC_SHORT_CODE=$(jq -r '.parameters.deploymentParams.value.loc_short_code' params.json)
GLOBAL_UNIQUENESS=$(jq -r '.parameters.deploymentParams.value.global_uniqueness' params.json)



RG_NAME="${ENTERPRISE_NAME}_${LOC_SHORT_CODE}_${ENTERPRISE_NAME_SUFFIX}_${GLOBAL_UNIQUENESS}"

DEPLOYMENT_NAME="${SUB_DEPLOYMENT_PREFIX}_${LOC_SHORT_CODE}_${ENTERPRISE_NAME_SUFFIX}_${GLOBAL_UNIQUENESS}_Deployment"

CLUSTER_NAME_PREFIX=$(jq -r '.parameters.aks_params.value.name_prefix' params.json)
CLUSTER_NAME="c-${CLUSTER_NAME_PREFIX}_${LOC_SHORT_CODE}_${ENTERPRISE_NAME_SUFFIX}_${GLOBAL_UNIQUENESS}"
CLUSTER_NAME=$(echo "${CLUSTER_NAME}" | sed 's/_/-/g')

function setup_kubeconfig() {
    echo -e "${YELLOW}Setting up kubeconfig for AKS cluster${RESET}"
    az aks get-credentials --resource-group ${RG_NAME} --name ${CLUSTER_NAME}

    if [ $? -eq 0 ]; then
        echo -e "${GREEN}Successfully setup kubeconfig for AKS cluster${RESET}"
    else
        echo -e "${RED}Failed to setup kubeconfig for AKS cluster${RESET}"
        exit 1
    fi
}

setup_kubeconfig