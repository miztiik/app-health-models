// SET MODULE DATE
param module_metadata object = {
  module_last_updated: '2023-11-06'
  owner: 'miztiik@github'
}

param deploymentParams object
param tags object

param logAnalyticsPayGWorkspaceId string

var __name_prefix = 'council'

var oai_svc_name = replace(
  '${deploymentParams.enterprise_name_suffix}-${deploymentParams.loc_short_code}-oai-${__name_prefix}-${deploymentParams.global_uniqueness}',
  '_',
  '-'
)

resource r_oai_svc 'Microsoft.CognitiveServices/accounts@2022-03-01' = {
  name: oai_svc_name
  location: deploymentParams.location
  tags: tags
  sku: {
    name: 'S0'
  }
  kind: 'OpenAI'
  identity: { type: 'SystemAssigned' }
  properties: {
    customSubDomainName: oai_svc_name
    networkAcls: {
      defaultAction: 'Allow'
    }
    publicNetworkAccess: 'Enabled'
    apiProperties: {
      statisticsEnabled: false
    }
  }
}

resource r_oai_deploy_chat 'Microsoft.CognitiveServices/accounts/deployments@2023-05-01' = {
  name: 'gpt-35-turbo-16k'
  sku: {
    capacity: 2
    name: 'Standard'
  }
  parent: r_oai_svc
  properties: {
    model: {
      format: 'OpenAI'
      name: 'gpt-35-turbo-16k'
      version: '0613'
    }
    raiPolicyName: 'Microsoft.Default'
    versionUpgradeOption: 'OnceNewDefaultVersionAvailable'
  }
}

resource r_oai_deploy_completions 'Microsoft.CognitiveServices/accounts/deployments@2023-05-01' = {
  name: 'text-embedding-ada-002'
  sku: {
    capacity: 1
    name: 'Standard'
  }
  parent: r_oai_svc
  properties: {
    model: {
      format: 'OpenAI'
      name: 'text-embedding-ada-002'
      version: '2'
    }
    raiPolicyName: 'Microsoft.Default'
    versionUpgradeOption: 'OnceNewDefaultVersionAvailable'
  }
}

/*

resource keyVault 'Microsoft.KeyVault/vaults@2023-02-01' existing = {
  name: keyVaultName
}

resource openAIkey 'Microsoft.KeyVault/vaults/secrets@2023-02-01' = {
  parent: keyVault
  name: 'azure-openai-key'
  properties: {
    contentType: 'Azure OpenAI Key'
    value: openAIaccount.listKeys().key1
  }
}

*/

// Create Diagnostic Settings
resource r_oai_svc_diag 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = {
  name: 'oai_svc_diag'
  scope: r_oai_svc
  properties: {
    workspaceId: logAnalyticsPayGWorkspaceId
    // logs: [
    //   {
    //     category: 'allLogs'
    //     enabled: true
    //   }
    // ]
    metrics: [
      {
        category: 'AllMetrics'
        enabled: true
      }
    ]
  }
}

// OUTPUTS
output module_metadata object = module_metadata

output oai_svc_name string = r_oai_svc.name
output oai_svc_endpoint string = r_oai_svc.properties.endpoint
output oai_svc_id string = r_oai_svc.id
