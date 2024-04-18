#!/bin/bash
# set -x
# set -e


# Define colors for console output
GREEN="\e[32m"
RED="\e[31m"
BLUE="\e[34m"
CYAN="\e[36m"
YELLOW="\e[33m"
RESET="\e[0m"
# echo -e "${GREEN}This text is green.${RESET}"

DASHBOARD_YAML_URI=" https://raw.githubusercontent.com/kubernetes/dashboard/v2.7.0/aio/deploy/recommended.yaml"

DASHBOARD_URL="http://localhost:8001/api/v1/namespaces/kubernetes-dashboard/services/https:kubernetes-dashboard:/proxy/"

echo -n "Get help here - https://kubernetes.io/docs/tasks/access-application-cluster/web-ui-dashboard/"

function deploy_dashboard(){

    echo -e "\n${RED} Delete existing deployments dashboard"
    kubectl delete -f ${DASHBOARD_YAML_URI}

    echo -e "${YELLOW} Deploying dashboard"
    kubectl apply -f ${DASHBOARD_YAML_URI}

    echo -e "${RED} Delete Default Role Bining"
    kubectl delete clusterrolebinding kubernetes-dashboard
    echo -e "${YELLOW} Creating clusterrolebinding kubernetes-dashboard"
# Create Admin Privileges
cat <<EOF | kubectl create -f -
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: kubernetes-dashboard
  namespace: kubernetes-dashboard
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: cluster-admin
subjects:
  - kind: ServiceAccount
    name: kubernetes-dashboard
    namespace: kubernetes-dashboard
EOF

    echo -e "${CYAN} Creating token for dashboard${RESET}"
    DASHBOARD_TOKEN=$(kubectl create token kubernetes-dashboard -n kubernetes-dashboard)



    echo -e "${RED} Terminating any existing proxy sessions${RESET}"
    ps -ef |grep -i kubectl|awk '{print $2}' | xargs -I {} kill -9 {}
    kubectl proxy &
    echo -e "${GREEN}Successfully deployed dashboard${RESET}"
    echo -e "Access Dashboard - ${GREEN}${DASHBOARD_URL}${RESET}"
    echo -e "DASHBOARD TOKEN: ${CYAN}${DASHBOARD_TOKEN}${RESET}"
}



function delete_dashboard()
{
    kubectl delete -f ${DASHBOARD_YAML_URI}

    kubectl delete clusterrolebinding kubernetes-dashboard

}

deploy_dashboard


