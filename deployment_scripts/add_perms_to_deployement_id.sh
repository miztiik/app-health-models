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

KEY_VAULT_NAME_PREFIX=$(jq -r '.parameters.key_vault_params.value.name_prefix' params.json)

KEY_VAULT_NAME=${KEY_VAULT_NAME_PREFIX}-${LOC_SHORT_CODE}-${GLOBAL_UNIQUENESS}


# Add Key Vault Policies
function add_kv_rbac_perms(){

    # https://learn.microsoft.com/en-us/azure/key-vault/general/rbac-guide?tabs=azure-cli
    LOGGED_IN_USER_PRINCIPAL_ID=$(az ad signed-in-user show --query id --out tsv)
    SUBSCRIPTION_ID=$(az account list --query "[?isDefault].[id]" --out tsv)
    # az role assignment create --role "00482a5a-887f-4fb3-b363-3b7fe8e74483" --assignee ${LOGGED_IN_USER_PRINCIPAL_ID} --scope /subscriptions/${SUBSCRIPTION_ID}/resourcegroups/${RG_NAME}
    
    az role assignment create \
        --role "Key Vault Administrator" \
        --assignee-object-id  ${LOGGED_IN_USER_PRINCIPAL_ID} \
        --scope "/subscriptions/${SUBSCRIPTION_ID}/resourcegroups/${RG_NAME}/providers/Microsoft.KeyVault/vaults/${KEY_VAULT_NAME}" \
        --assignee-principal-type User


    if [ $? -eq 0 ]; then
        echo -e "${GREEN}Successfully added RBAC permissions to Key Vault${RESET}"
    else
        echo -e "${RED}Failed to add RBAC permissions to Key Vault${RESET}"
    fi
}

add_kv_rbac_perms


