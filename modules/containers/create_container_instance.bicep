// SET MODULE DATE
param module_metadata object = {
  module_last_updated: '2023-06-25'
  owner: 'miztiik@github'
}

param deploymentParams object
param tags object

param uami_name_akane string
param logAnalyticsWorkspaceName string

param container_instance_params object
param acr_name string

param svc_bus_ns_name string
param svc_bus_q_name string

param saName string
param blobContainerName string

param cosmos_db_accnt_name string
param cosmos_db_name string
param cosmos_db_container_name string

@description('Get Storage Account Reference')
resource r_sa 'Microsoft.Storage/storageAccounts@2021-06-01' existing = {
  name: saName
}

@description('Get Cosmos DB Account Ref')
resource r_cosmos_db_accnt 'Microsoft.DocumentDB/databaseAccounts@2022-08-15' existing = {
  name: cosmos_db_accnt_name
}

@description('Get Log Analytics Workspace Reference')
resource r_logAnalyticsPayGWorkspace_ref 'Microsoft.OperationalInsights/workspaces@2022-10-01' existing = {
  name: logAnalyticsWorkspaceName
}

@description('Reference existing User-Assigned Identity')
resource r_uami_container_app 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' existing = {
  name: uami_name_akane
}

@description('Get Container Registry Reference')
resource r_acr 'Microsoft.ContainerRegistry/registries@2022-02-01-preview' existing = {
  name: acr_name
}

var _c_grp_name = replace('${deploymentParams.enterprise_name_suffix}-${deploymentParams.loc_short_code}-${container_instance_params.name_prefix}-${deploymentParams.global_uniqueness}', '_', '')

resource r_c_grp_producer 'Microsoft.ContainerInstance/containerGroups@2021-09-01' = {
  name: '${_c_grp_name}-producer'
  location: deploymentParams.location
  tags: tags
  // zones: [ '1' ] //"Availability Zones are not available in location: 'northeurope'
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${r_uami_container_app.id}': {}
    }
  }
  properties: {
    containers: [
      {
        name: 'miztiik-event-producer'
        properties: {
          environmentVariables: [
            {
              name: 'APPINSIGHTS_INSTRUMENTATIONKEY'
              value: r_logAnalyticsPayGWorkspace_ref.properties.customerId
            }
            {
              name: 'APP_ROLE'
              value: 'producer'
            }
            {
              name: 'SVC_BUS_FQDN'
              value: '${svc_bus_ns_name}.servicebus.windows.net'
            }
            {
              name: 'SVC_BUS_Q_NAME'
              value: svc_bus_q_name
            }
            {
              name: 'AZURE_CLIENT_ID'
              value: r_uami_container_app.properties.clientId
            }
            {
              name: 'TOT_MSGS_TO_PRODUCE'
              value: '15'
            }
          ]
          image: '${sys.toLower(acr_name)}.azurecr.io/miztiik/event-processor-for-svc-bus-q:latest'
          ports: [
            {
              port: 80
              protocol: 'TCP'
            }
          ]
          resources: {
            requests: {
              cpu: 1
              memoryInGB: 2
            }
          }
        }
      }
    ]
    osType: 'Linux'
    restartPolicy: 'Always'
    imageRegistryCredentials: [
      {
        username: r_acr.listCredentials().username
        server: r_acr.properties.loginServer
        password: r_acr.listCredentials().passwords[0].value
      }
    ]
    diagnostics: {
      logAnalytics: {
        logType: 'ContainerInsights'
        workspaceId: r_logAnalyticsPayGWorkspace_ref.properties.customerId
        workspaceKey: r_logAnalyticsPayGWorkspace_ref.listKeys().primarySharedKey
      }
    }
    ipAddress: {
      type: 'Public'
      dnsNameLabel: '${_c_grp_name}-producer'
      ports: [
        {
          port: 80
          protocol: 'TCP'
        }
      ]
    }
  }
}

resource r_c_grp_consumer 'Microsoft.ContainerInstance/containerGroups@2021-09-01' = {
  name: '${_c_grp_name}-consumer'
  location: deploymentParams.location
  tags: tags
  // zones: [ '1' ] //"Availability Zones are not available in location: 'northeurope'
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${r_uami_container_app.id}': {}
    }
  }
  properties: {
    containers: [
      {
        name: 'miztiik-event-consumer'
        properties: {
          environmentVariables: [
            // Needed for Managed Identity To Work
            {
              name: 'AZURE_CLIENT_ID'
              value: r_uami_container_app.properties.clientId
            }
            {
              name: 'APP_ROLE'
              value: 'consumer'
            }
            {
              name: 'APPINSIGHTS_INSTRUMENTATIONKEY'
              value: r_logAnalyticsPayGWorkspace_ref.properties.customerId
            }
            {
              name: 'SVC_BUS_FQDN'
              value: '${svc_bus_ns_name}.servicebus.windows.net'
            }
            {
              name: 'SVC_BUS_Q_NAME'
              value: svc_bus_q_name
            }
            {
              name: 'SA_NAME'
              value: r_sa.name
            }
            {
              name: 'BLOB_SVC_ACCOUNT_URL'
              value: r_sa.properties.primaryEndpoints.blob
            }
            {
              name: 'BLOB_NAME'
              value: blobContainerName
            }
            {
              name: 'COSMOS_DB_URL'
              value: r_cosmos_db_accnt.properties.documentEndpoint
            }
            {
              name: 'COSMOS_DB_NAME'
              value: cosmos_db_name
            }
            {
              name: 'COSMOS_DB_CONTAINER_NAME'
              value: cosmos_db_container_name
            }
            {
              name: 'MAX_MSGS_TO_PROCESS'
              value: '10'
            }
          ]
          image: '${sys.toLower(acr_name)}.azurecr.io/miztiik/event-processor-for-svc-bus-q:latest'
          ports: [
            {
              port: 80
              protocol: 'TCP'
            }
          ]
          resources: {
            requests: {
              cpu: 1
              memoryInGB: 2
            }
          }
        }
      }
    ]
    osType: 'Linux'
    restartPolicy: 'Always'
    imageRegistryCredentials: [
      {
        username: r_acr.listCredentials().username
        server: r_acr.properties.loginServer
        password: r_acr.listCredentials().passwords[0].value
      }
    ]
    diagnostics: {
      logAnalytics: {
        logType: 'ContainerInsights'
        workspaceId: r_logAnalyticsPayGWorkspace_ref.properties.customerId
        workspaceKey: r_logAnalyticsPayGWorkspace_ref.listKeys().primarySharedKey
      }
    }
    ipAddress: {
      type: 'Public'
      dnsNameLabel: '${_c_grp_name}-consumer'
      ports: [
        {
          port: 80
          protocol: 'TCP'
        }
      ]
    }
  }
}

// Assign the Cosmos Data Plane Owner role to the user-assigned managed identity
var cosmosDbDataContributor_RoleDefinitionId = resourceId('Microsoft.DocumentDB/databaseAccounts/sqlRoleDefinitions', r_cosmos_db_accnt.name, '00000000-0000-0000-0000-000000000002')

resource r_customRoleAssignmentToUsrIdentity 'Microsoft.DocumentDB/databaseAccounts/sqlRoleAssignments@2021-04-15' = {
  name: guid(r_uami_container_app.id, r_cosmos_db_accnt.id, cosmosDbDataContributor_RoleDefinitionId)
  parent: r_cosmos_db_accnt
  properties: {
    // roleDefinitionId: r_cosmodb_customRoleDef.id
    roleDefinitionId: cosmosDbDataContributor_RoleDefinitionId
    scope: r_cosmos_db_accnt.id
    principalId: r_uami_container_app.properties.principalId
  }
  dependsOn: [
    r_uami_container_app
  ]
}

// OUTPUTS
output module_metadata object = module_metadata

output fqdn string = r_c_grp_producer.properties.ipAddress.fqdn

output consumer_fqdn string = r_c_grp_consumer.properties.ipAddress.fqdn
