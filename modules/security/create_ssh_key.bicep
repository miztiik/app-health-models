// SET MODULE DATE
param module_metadata object = {
  module_last_updated: '2023-07-01'
  owner: 'miztiik@github'
}

param deploymentParams object
param key_vault_name string
param tags object

param uami_name_akane string

@description('Get existing User-Assigned Identity')
resource r_uami_ref 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' existing = {
  name: uami_name_akane
}

@description('Get Key Vault Resource Ref')
resource r_kv_ref 'Microsoft.KeyVault/vaults@2021-11-01-preview' existing = {
  name: key_vault_name
}

param ssh_key_name string = 'miztiik-ssh-key'

// The value of AZ_SCRIPTS_OUTPUT_PATH is /mnt/azscripts/azscriptoutput/scriptoutputs.json
//outputs must be a valid JSON string object. The contents of the file must be saved as a key-value pair.  

resource r_ssh_key_deployment_script 'Microsoft.Resources/deploymentScripts@2020-10-01' = {
  name: 'miztiik-ssh-key-deployment-script'
  location: deploymentParams.location
  tags: tags
  kind: 'AzureCLI'
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${r_uami_ref.id}': {}
    }
  }
  properties: {
    azCliVersion: '2.37.0'
    timeout: 'PT5M'
    retentionInterval: 'P1D'
    cleanupPreference: 'OnSuccess'
    //https://github.com/jwendl/bicep-iot-edge-device-vm/blob/b1a5e5f146a905b8a7f83e0655957c5aef1f06f6/deploy/bicep/modules/ssh-keys.bicep#L23
    scriptContent: '''
      #!/bin/bash
      set -euxo pipefail
      KV_INJECT_STATUS="NO_KEY_EXISTS"
      SSH_PUB_KEY=""
      SSH_PVT_KEY=""
      DATE_TIME=""

      if az keyvault secret show --vault-name  ${KEY_VAULT_NAME} --name "${SSH_KEY_NAME}-pub" --output none
        then
          echo "SSH Key found in Key Vault. Fetching the key pair"
          SSH_PUB_KEY=$(az keyvault secret show --vault-name  ${KEY_VAULT_NAME} --name "${SSH_KEY_NAME}-pub" --query value --output tsv)
          SSH_PVT_KEY=$(az keyvault secret show --vault-name  ${KEY_VAULT_NAME} --name "${SSH_KEY_NAME}-pvt" --query value --output tsv)
          KV_INJECT_STATUS="KEY_EXISTS"
        else
            echo "SSH Key not found in Key Vault. Generating new key pair"
            ssh-keygen -t rsa -b 4096 -N "" -f ~/.ssh/id_rsa  -C "miztiik@git"
            # ssh-keygen -t rsa -m pem -b 4096 -N "" -f ~/.ssh/id_rsa  -C "miztiik@git" # For PEM Format
      
            SSH_PUB_KEY=$(cat ~/.ssh/id_rsa.pub)
            SSH_PVT_KEY=$(cat ~/.ssh/id_rsa)

            az keyvault secret set --name "${SSH_KEY_NAME}-pub" --vault-name ${KEY_VAULT_NAME} --value "${SSH_PUB_KEY}"
            az keyvault secret set --name "${SSH_KEY_NAME}-pvt" --vault-name ${KEY_VAULT_NAME} --value "${SSH_PVT_KEY}"

          KV_INJECT_STATUS="NEW_KEY_CREATED"
      fi
      
      # Save the output to a file
      cat <<EOF >$AZ_SCRIPTS_OUTPUT_PATH
      {
        "KV_INJECT_STATUS": "${KV_INJECT_STATUS}",
        "SSH_PUB_KEY": "${SSH_PUB_KEY}",
        "SSH_PVT_KEY": "${SSH_PVT_KEY}",
        "DATE_TIME": "$(date)"
      }
      EOF
    '''
    arguments: '-v'
    environmentVariables: [
      {
        name: 'AZURE_KEYVAULT_URI'
        value: r_kv_ref.properties.vaultUri
      }
      {
        name: 'SSH_KEY_NAME'
        value: ssh_key_name
      }
      {
        name: 'KEY_VAULT_NAME'
        value: key_vault_name
      }
    ]
  }
  dependsOn: [
    r_kv_ref
  ]
}

// OUTPUTS
output module_metadata object = module_metadata

output SSH_PUB_KEY string = r_ssh_key_deployment_script.properties.outputs.SSH_PUB_KEY
