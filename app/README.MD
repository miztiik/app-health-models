# Azure Python Function Local Environment Setup

```bash
FUNC_DIR="function_code"
FUNC_PROJ_NAME="store-backend-ops"
FUNC_APP_NAME="store-backend-fnApp-006"
FUNC_NAME="store-events-consumer-fn"

cd app/
mkdir -p ${FUNC_DIR}
cd ${FUNC_DIR}
python -m venv .venv
source .venv/Scripts/activate

func init ${FUNC_PROJ_NAME} --python -m V1
cd ${FUNC_PROJ_NAME}

# Create function from template
func new --name ${FUNC_NAME} --template "Azure Event Grid trigger"

# Publishing the function to Azure Functions
# func azure functionapp publish ${FUNC_APP_NAME} --nozip



# https://learn.microsoft.com/en-us/azure/azure-functions/functions-run-local?tabs=windows%2Cisolated-process%2Cnode-v4%2Cpython-v2%2Chttp-trigger%2Ccontainer-apps&pivots=programming-language-python

# Create a function app

func init MyProjFolder --worker-runtime python --model V2
func new --name MyHttpTrigger --template "Http Trigger" --authlevel "anonymous" --worker-runtime python

func azure functionapp publish MyHttpTrigger

func settings decrypt


```