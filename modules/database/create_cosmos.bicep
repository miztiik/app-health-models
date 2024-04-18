// SET MODULE DATE
param module_metadata object = {
  module_last_updated: '2024-01-04'
  owner: 'miztiik@github'
}

param deploymentParams object
param tags object

param cosmosdb_params object
param logAnalyticsWorkspaceId string

var cosmos_db_accnt_name = replace(
  '${deploymentParams.enterprise_name_suffix}-${deploymentParams.loc_short_code}-db-account-${deploymentParams.global_uniqueness}',
  '_',
  '-'
)

@description('Create CosmosDB Account')
resource r_cosmos_db_account 'Microsoft.DocumentDB/databaseAccounts@2022-08-15' = {
  name: cosmos_db_accnt_name
  location: deploymentParams.location
  kind: 'GlobalDocumentDB'
  tags: tags
  properties: {
    publicNetworkAccess: 'Enabled'
    databaseAccountOfferType: 'Standard'
    enableAutomaticFailover: true
    consistencyPolicy: {
      defaultConsistencyLevel: 'Session'
    }
    locations: [
      {
        locationName: deploymentParams.location
        isZoneRedundant: false
      }
    ]

    backupPolicy: {
      type: 'Continuous'
    }
    capabilities: [
      {
        name: 'EnableServerless'
      }
    ]
  }
}

@description('Create CosmosDB Database')
var databaseName = '${cosmosdb_params.name_prefix}-db-${deploymentParams.global_uniqueness}'

resource r_cosmos_db 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases@2021-06-15' = {
  parent: r_cosmos_db_account
  name: databaseName
  properties: {
    resource: {
      id: databaseName
    }
  }
}

@description('Create CosmosDB Container')
var containerName = '${cosmosdb_params.name_prefix}-container-${deploymentParams.global_uniqueness}'
resource r_cosmos_db_container 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers@2022-08-15' = {
  name: containerName
  parent: r_cosmos_db
  properties: {
    resource: {
      id: containerName
      partitionKey: {
        paths: [
          '/id'
        ]
        kind: 'Hash'
      }
      indexingPolicy: {
        indexingMode: 'consistent'
        includedPaths: [
          {
            path: '/*'
          }
        ]
        excludedPaths: [
          {
            path: '/_etag/?'
          }
        ]
      }
      conflictResolutionPolicy: {
        mode: 'LastWriterWins'
        conflictResolutionPath: '/_ts'
      }
    }
  }
}
////////////////////////////////////////////
//                                        //
//         Diagnostic Settings            //
//                                        //
////////////////////////////////////////////
resource r_cosmos_db_account_diags 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = {
  name: '${cosmos_db_accnt_name}-diags'
  scope: r_cosmos_db_account
  properties: {
    workspaceId: logAnalyticsWorkspaceId
    logAnalyticsDestinationType: 'Dedicated'
    logs: [
      {
        categoryGroup: 'audit'
        enabled: true
      }
      {
        categoryGroup: 'allLogs'
        enabled: true
      }
    ]
    metrics: [
      {
        category: 'Requests'
        enabled: true
      }
    ]
  }
}

// OUTPUTS
output module_metadata object = module_metadata

output cosmos_db_accnt_name string = r_cosmos_db_account.name
output cosmos_db_name string = r_cosmos_db.name
output cosmos_db_container_name string = r_cosmos_db_container.name
