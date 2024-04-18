.PHONY: test help clean
.DEFAULT_GOAL := help

# Global Variables
CURRENT_PWD:=$(shell pwd)
VENV_DIR:=.env


help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

all: ## Trigger Resources and Function Code deployments
	make deploy
	make func
	make docker

deploy: ## Trigger Only Resource deployments & Not Function Code
	sh deployment_scripts/deploy.sh

func: ## Trigger Only Funtion code deployments
	sh deployment_scripts/deploy_func_v2.sh

docker: ## Build docker image
	sh app/container_builds/event_processor_for_svc_bus_queues/build_and_push_img.sh

powerup: ## Add permissions to deployment id (NOT WORKING - WIP)
	sh deployment_scripts/add_perms_to_deployement_id.sh

spice: ## Deploy k8s_utils
	sh app/k8s_utils/bootstrap_cluster/setup_kubeconfig.sh
	sh app/k8s_utils/bootstrap_cluster/deploy_dashboard.sh

destroy: ## Delete deployments without confirmation
	sh deployment_scripts/destroy.sh shiva

clean: ## Remove All virtualenvs
	@rm -rf ${PWD}/${VENV_DIR} build *.egg-info .eggs .pytest_cache .coverage
	@find . | grep -E "(__pycache__|\.pyc|\.pyo$$)" | xargs rm -rf