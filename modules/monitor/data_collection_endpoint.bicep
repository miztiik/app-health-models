// SET MODULE DATE
param module_metadata object = {
  module_last_updated: '2023-12-17'
  owner: 'miztiik@github'
}
param deploymentParams object
param dce_params object
param tags object

@description('Create a Data Collection Endpoint for Azure Log Analytics Workspace')
var lin_dce_name = replace('${deploymentParams.enterprise_name_suffix}-${deploymentParams.loc_short_code}-${dce_params.name_prefix}-dce-${deploymentParams.global_uniqueness}', '_', '-')

resource r_lin_dce 'Microsoft.Insights/dataCollectionEndpoints@2021-04-01' = {
  name: lin_dce_name
  location: deploymentParams.location
  tags: tags
  kind: 'Linux'
  properties: {
    networkAcls: {
      publicNetworkAccess: 'Enabled'
    }
  }
}

// OUTPUTS
output module_metadata object = module_metadata

output linux_dce_id string = r_lin_dce.id
output linux_dce_name string = r_lin_dce.name
