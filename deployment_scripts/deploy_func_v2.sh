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


# var r_fn_app_name = replace('${deploymentParams.loc_short_code}-${deploymentParams.enterprise_name_suffix}-${funcParams.funcAppPrefix}-fn-app-${deploymentParams.global_uniqueness}', '_', '-')

FUNC_APP_NAME_PART_1=$(jq -r '.parameters.deploymentParams.value.enterprise_name_suffix' params.json)
FUNC_APP_NAME_PART_2=$(jq -r '.parameters.fn_params.value.name_prefix' params.json)
FUNC_APP_NAME_PART_3="fn-app"
GLOBAL_UNIQUENESS=$(jq -r '.parameters.deploymentParams.value.global_uniqueness' params.json)
FUNC_APP_NAME=${FUNC_APP_NAME_PART_1}-${FUNC_APP_NAME_PART_2}-${LOC_SHORT_CODE}-${FUNC_APP_NAME_PART_3}-${GLOBAL_UNIQUENESS}
FUNC_APP_NAME="${FUNC_APP_NAME//_/-}"

# var __apim_name = replace('${deploymentParams.enterprise_name_suffix}-store-front-${deploymentParams.loc_short_code}-apim-${deploymentParams.global_uniqueness}', '_', '-')

APIM_NAME=${ENTERPRISE_NAME_SUFFIX}-store-front-${LOC_SHORT_CODE}-apim-${GLOBAL_UNIQUENESS}

PRODUCER_FUNC_NAME="store_events_producer"
CONSUMER_FUNC_NAME="store_events_consumer"

# echo "$FUNC_APP_NAME"

FUNC_CODE_LOCATION="./app/function_code/store-backend-ops-v2/"
pushd ${FUNC_CODE_LOCATION}

# Publish the function App
function deploy_event_producer_code(){
    
    # pushd ${PRODUCER_FUNC_NAME}

    # Initiate Deployments
    echo -e "${YELLOW} Initiating Python Function Deployment ${RESET}" 
    echo -e "  Deploying code from ${CYAN}${PRODUCER_FUNC_NAME}${RESET} to FnApp: ${CYAN}${FUNC_APP_NAME}${RESET}"

    func azure functionapp publish ${FUNC_APP_NAME} --nozip --python

    if [ $? -eq 0 ]; then
        echo -e "${GREEN}Function ${PRODUCER_FUNC_NAME} Deployment Successful${RESET}"
    else
        echo -e "${RED}Function ${PRODUCER_FUNC_NAME} Deployment Failed${RESET}"
        exit 1
    fi

    # popd
}

function deploy_event_consumer_code(){
    
    pushd ${CONSUMER_FUNC_NAME}

    # Initiate Deployments
    echo -e "${YELLOW} Initiating Python Function Deployment ${RESET}" 
    echo -e "  Deploying code from ${CYAN}${CONSUMER_FUNC_NAME}${RESET} to FnApp: ${CYAN}${FUNC_APP_NAME}${RESET}"

    func azure functionapp publish ${FUNC_APP_NAME} --nozip --python

    if [ $? -eq 0 ]; then
        echo -e "${GREEN}Function ${CONSUMER_FUNC_NAME} Deployment Successful${RESET}"
    else
        echo -e "${RED}Function ${CONSUMER_FUNC_NAME} Deployment Failed${RESET}"
        exit 1
    fi

    popd
}


function load_local_config(){
    func azure functionapp fetch-app-settings ${FUNC_APP_NAME}
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}Function App Settings Loaded Successfully${RESET}"
    else
        echo -e "${RED}Function App Settings Load Failed${RESET}"
        exit 1
    fi
}


#########################################################################
#########################################################################
#########################################################################
#########################################################################


deploy_event_producer_code
# deploy_event_consumer_code
load_local_config > /dev/null 2>&1



# Return to original directory
popd

echo -e " Producer Fn URL: ${CYAN}https://${FUNC_APP_NAME}.azurewebsites.net/miztiik_automation/${PRODUCER_FUNC_NAME}${RESET}"
echo -e " Producer API URL: ${CYAN}https://${APIM_NAME}.azure-api.net/api/miztiik_automation/${PRODUCER_FUNC_NAME}${RESET}"
