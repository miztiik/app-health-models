// SET MODULE DATE
param module_metadata object = {
  module_last_updated: '2023-11-06'
  owner: 'miztiik@github'
}

param deploymentParams object
param tags object

param logAnalyticsPayGWorkspaceId string

var __name_prefix = 'council'

var oai_svc_name = replace('${deploymentParams.enterprise_name_suffix}-${deploymentParams.loc_short_code}-oai-${__name_prefix}-${deploymentParams.global_uniqueness}', '_', '-')

resource r_oai_svc 'Microsoft.CognitiveServices/accounts@2022-03-01' = {
  name: oai_svc_name
  location: deploymentParams.location
  tags: tags
  sku: {
    name: 'S0'
  }
  kind: 'OpenAI'
  properties: {
    apiProperties: {
      statisticsEnabled: false
    }
  }
}

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
