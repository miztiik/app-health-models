.PHONY: test help clean
.DEFAULT_GOAL := help

# Global Variables
CURRENT_PWD:=$(shell pwd)
VENV_DIR:=.env


# Set Global Variables
MAIN_BICEP_TEMPL_NAME="main.bicep"
LOCATION=$(shell jq -r '.parameters.deploymentParams.value.location' params.json)
SUB_DEPLOYMENT_PREFIX=$(shell jq -r '.parameters.deploymentParams.value.sub_deploymnet_prefix' params.json)
ENTERPRISE_NAME=$(shell jq -r '.parameters.deploymentParams.value.enterprise_name' params.json)
ENTERPRISE_NAME_SUFFIX=$(shell jq -r '.parameters.deploymentParams.value.enterprise_name_suffix' params.json)
LOC_SHORT_CODE=$(shell jq -r '.parameters.deploymentParams.value.loc_short_code' params.json)
GLOBAL_UNIQUENESS=$(shell jq -r '.parameters.deploymentParams.value.global_uniqueness' params.json)

RG_NAME="$(ENTERPRISE_NAME)_$(LOC_SHORT_CODE)_$(ENTERPRISE_NAME_SUFFIX)_$(GLOBAL_UNIQUENESS)"

DEPLOYMENT_NAME="${SUB_DEPLOYMENT_PREFIX}_${LOC_SHORT_CODE}_${ENTERPRISE_NAME_SUFFIX}_${GLOBAL_UNIQUENESS}_Deployment"

SA_NAME=$(shell az storage account list --resource-group $(RG_NAME) --query "[?starts_with(name, 'ware')].name" -o tsv)


CONTAINER_NAME="analysts-reports"

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

all: ## Trigger Resources and Function Code deployments
	make deploy
	make func
	make docker

deploy: ## Trigger Only Resource deployments & Not Function Code
	@echo "🚀 Create Resource Group and Deploy Resources in it..."
	sh deployment_scripts/deploy.sh

func: ## Trigger Only Funtion code deployments
	sh deployment_scripts/deploy_func_v2.sh

docker: ## Build docker image
	sh deployment_scripts/build_and_push_img.sh

powerup: ## Add permissions to deployment id (NOT WORKING - WIP)
	sh deployment_scripts/add_perms_to_deployement_id.sh

spice: ## Deploy k8s_utils
	sh app/k8s_utils/bootstrap_cluster/setup_kubeconfig.sh
	sh app/k8s_utils/bootstrap_cluster/deploy_dashboard.sh

destroy: ## Delete deployments without confirmation
	@echo "⚡⚠️ I hope you are sure to delete everything?⚡"
	sh deployment_scripts/destroy.sh shiva

clean: ## Remove All virtualenvs
	@rm -rf ${PWD}/${VENV_DIR} build *.egg-info .eggs .pytest_cache .coverage
	@find . | grep -E "(__pycache__|\.pyc|\.pyo$$)" | xargs rm -rf


copy-to-blob: ## Copy a PDF file to containter `analysts-reports`
	@echo "📦 Copying resources to Azure storage account..."
	az storage container create \
		--account-name $(SA_NAME) \
		--name $(CONTAINER_NAME)

	
	az storage blob upload \
		--account-name $(SA_NAME) \
		--container-name $(CONTAINER_NAME) \
		--no-progress \
		--overwrite \
		--name mckinsey-building-the-ai-bank-of-the-future.pdf \
		--file datasets/raw_data/pdf/analysts_reports/mckinsey-building-the-ai-bank-of-the-future.pdf

copy-bulk-to-blob: ## Copy directory to blob
	@echo "📦 Copying Multiple files to Azure storage account..."
	az storage container create \
		--account-name $(SA_NAME) \
		--name $(CONTAINER_NAME)

	az storage blob directory upload \
		--account-name $(SA_NAME) \
		--container $(CONTAINER_NAME) \
		--only-show-errors \
		--recursive \
		--source "datasets/raw_data/pdf/analysts_reports/*" \
		--destination-path .

logs: ## Fetch ContainerAppLogs
	az containerapp logs show \
		--follow \
		--format text \
		--name claim-ai \
		--resource-group $(name) \
		--tail 100

logs-history: ## Fetch Log history
	log_analytics_workspace_customer_id ?= $(shell az deployment sub show --name $(name) | yq '.properties.outputs["logAnalyticsWorkspaceName"].value')
	az monitor log-analytics query \
		--analytics-query "ContainerAppConsoleLogs_CL | project TimeGenerated, Log_s | sort by TimeGenerated desc" \
		--output jsonc \
		--timespan P1D \
		--workspace $(log_analytics_workspace_customer_id)