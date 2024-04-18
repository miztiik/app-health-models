// SET MODULE DATE
param module_metadata object = {
  module_last_updated: '2024-01-05'
  owner: 'miztiik@github'
}

param deploymentParams object
param uami_name_akane string

param tags object

param logAnalyticsPayGWorkspaceId string

@description('Get function existing User-Assigned Managed Identity')
resource r_uami_akane 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' existing = {
  name: uami_name_akane
}

@description('Create Azure Cognitive Search Service')
var __name_prefix = 'sage-library'

var cognitive_search_name = replace('${deploymentParams.enterprise_name_suffix}-${deploymentParams.loc_short_code}-cog-search-${__name_prefix}-${deploymentParams.global_uniqueness}', '_', '-')

resource r_cognitive_search 'Microsoft.Search/searchServices@2021-04-01-preview' = {
  name: cognitive_search_name
  location: deploymentParams.location
  tags: tags
  sku: {
    name: 'standard'
    // name: 'basic'
    // name: 'storage_optimized_l2'
  }
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${r_uami_akane.id}': {}
    }
  }
  properties: {
    replicaCount: 1
    partitionCount: 1
    hostingMode: 'default'
    // hostingMode: 'highDensity'
  }
}

@description('Create Diagnostic Settings for Cognitive Search')
resource r_cog_search_diag 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = {
  name: 'cog_search_diag'
  scope: r_cognitive_search
  properties: {
    workspaceId: logAnalyticsPayGWorkspaceId
    logs: [
      {
        category: 'OperationLogs'
        enabled: true
      }
    ]
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

output cognitive_search_name string = r_cognitive_search.name
