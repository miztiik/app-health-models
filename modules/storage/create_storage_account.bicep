// SET MODULE DATE
param module_metadata object = {
  module_last_updated: '2023-12-16'
  owner: 'miztiik@github'
}

param deploymentParams object
param sa_params object
param tags object = resourceGroup().tags

param enableDiagnostics bool = true
param logAnalyticsWorkspaceId string

// var = uniqStr2 = guid(resourceGroup().id, "asda")
var uniqStr = substring(uniqueString(resourceGroup().id), 0, 6)
var saName = '${sa_params.name_prefix}${deploymentParams.loc_short_code}${uniqStr}${deploymentParams.global_uniqueness}'

// Storage Account - Store Events - Warehouse
resource r_sa 'Microsoft.Storage/storageAccounts@2022-05-01' = {
  name: saName
  location: deploymentParams.location
  tags: tags
  sku: {
    name: 'Standard_GRS'
  }
  kind: 'StorageV2'
  properties: {
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
    defaultToOAuthAuthentication: true
    supportsHttpsTrafficOnly: true
    networkAcls: {
      bypass: 'AzureServices'
      defaultAction: 'Allow'
    }
    encryption: {
      services: {
        file: {
          keyType: 'Account'
          enabled: true
        }
        blob: {
          keyType: 'Account'
          enabled: true
        }
      }
      keySource: 'Microsoft.Storage'
    }

    accessTier: 'Hot'
  }
}

// var = uniqStr2 = guid(resourceGroup().id, "asda")
var uniqStr_1 = substring(uniqueString(resourceGroup().id), 0, 6)
var misc_sa_name = '${sa_params.misc_sa_name_prefix}${uniqStr_1}${deploymentParams.global_uniqueness}'

// Storage Account for Function Code Storage, VM Diagnostics, and Azure Backup
resource misc_sa 'Microsoft.Storage/storageAccounts@2022-05-01' = {
  name: misc_sa_name
  location: deploymentParams.location
  tags: tags
  sku: {
    name: 'Standard_GRS'
  }
  kind: 'StorageV2'
  properties: {
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
    defaultToOAuthAuthentication: true
    supportsHttpsTrafficOnly: true
    networkAcls: {
      bypass: 'AzureServices'
      defaultAction: 'Allow'
    }
    encryption: {
      services: {
        file: {
          keyType: 'Account'
          enabled: true
        }
        blob: {
          keyType: 'Account'
          enabled: true
        }
      }
      keySource: 'Microsoft.Storage'
    }

    accessTier: 'Hot'
  }
}

////////////////////////////////////////////
//                                        //
//         Diagnostic Settings            //
//                                        //
////////////////////////////////////////////

@description('Enabling Diagnostics for the storage account')
resource r_sa_diags 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = if (enableDiagnostics) {
  name: '${saName}-diags'
  scope: r_sa
  properties: {
    workspaceId: logAnalyticsWorkspaceId
    metrics: [
      {
        category: 'Transaction'
        enabled: true
      }
    ]
    logAnalyticsDestinationType: 'Dedicated'
  }
}

resource misc_sa_diags 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = if (enableDiagnostics) {
  name: '${misc_sa_name}-diags'
  scope: misc_sa
  properties: {
    workspaceId: logAnalyticsWorkspaceId
    metrics: [
      {
        category: 'Transaction'
        enabled: true
      }
    ]
    logAnalyticsDestinationType: 'Dedicated'
  }
}

// OUTPUTS
output module_metadata object = module_metadata

output sa_name string = r_sa.name
output sa_primary_blob_endpoints string = r_sa.properties.primaryEndpoints.blob
output sa_primary_endpoints object = r_sa.properties.primaryEndpoints

output misc_sa_name string = misc_sa.name
