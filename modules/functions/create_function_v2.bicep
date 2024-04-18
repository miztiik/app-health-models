// SET MODULE DATE
param module_metadata object = {
  module_last_updated: '2024-01-04'
  owner: 'miztiik@github'
}

param deploymentParams object
param fn_params object
param tags object
param laws_id string
param alert_action_group_id string

param uami_name_func string

param sa_name string
param misc_sa_name string

param blob_container_name string

param svc_bus_ns_name string
param svc_bus_q_name string
param svc_bus_topic_name string
param all_events_subscriber_name string
param sales_events_subscriber_name string

param cosmos_db_accnt_name string
param cosmos_db_name string
param cosmos_db_container_name string

// Get Storage Account Reference
resource r_sa_ref 'Microsoft.Storage/storageAccounts@2021-06-01' existing = {
  name: sa_name
}

@description('Get function Storage Account Reference')
resource r_misc_sa_ref 'Microsoft.Storage/storageAccounts@2021-06-01' existing = {
  name: misc_sa_name
}

@description('Get function existing User-Assigned Managed Identity')
resource r_uami_func 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' existing = {
  name: uami_name_func
}

@description('Get Cosmos DB Account Ref')
resource r_cosmos_db_accnt 'Microsoft.DocumentDB/databaseAccounts@2022-08-15' existing = {
  name: cosmos_db_accnt_name
}

// @description('Get Service Bus Namespace Reference')
// resource r_svc_bus_ns_ref 'Microsoft.ServiceBus/namespaces@2022-01-01-preview' existing = {
//   name: svc_bus_ns_name
// }

// Add permissions to the Function App identity
// Azure Built-In Roles Ref: https://learn.microsoft.com/en-us/azure/role-based-access-control/built-in-roles

@description('List of built-in roles and their IDs')
var built_in_roles = [
  { name: 'Storage Blob Data Contributor', id: 'ba92f5b4-2d11-453d-a403-e96b0029c9fe' }
  { name: 'Azure Service Bus Data Owner', id: '090c5cfd-751d-490a-894a-3ce6f1109419' }
  { name: 'Azure Event Hubs Data Owner', id: 'f526a384-b230-433a-b45c-95f59c4a2dec' }
  { name: 'Log Analytics Contributor', id: '92aaf0da-9dab-42b6-94a3-d43ce8d16293' }
  { name: 'Monitoring Contributor', id: '749f88d5-cbae-40b8-bcfc-e573ddc772fa' }
  { name: 'Monitoring Metrics Publisher', id: '3913510d-42f4-4e42-8a64-420c390055eb' }
]

@description('Assign the Permissions to Role')
resource role_assignment 'Microsoft.Authorization/roleAssignments@2020-04-01-preview' = [
  for role in built_in_roles: {
    name: guid(r_uami_func.id, resourceGroup().id, role.name)
    properties: {
      roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', role.id)
      principalId: r_uami_func.properties.principalId
    }
  }
]

// Assign the Cosmos Data Plane Owner role to the user-assigned managed identity
var cosmosDbDataContributor_RoleDefinitionId = resourceId(
  'Microsoft.DocumentDB/databaseAccounts/sqlRoleDefinitions',
  r_cosmos_db_accnt.name,
  '00000000-0000-0000-0000-000000000002'
)

resource r_customRoleAssignmentToUsrIdentity 'Microsoft.DocumentDB/databaseAccounts/sqlRoleAssignments@2021-04-15' = {
  name: guid(r_uami_func.id, r_cosmos_db_accnt.id, cosmosDbDataContributor_RoleDefinitionId, r_sa_ref.id)
  parent: r_cosmos_db_accnt
  properties: {
    // roleDefinitionId: r_cosmodb_customRoleDef.id
    roleDefinitionId: cosmosDbDataContributor_RoleDefinitionId
    scope: r_cosmos_db_accnt.id
    principalId: r_uami_func.properties.principalId
  }
  dependsOn: [
    r_uami_func
  ]
}

@description('Create Application Insights')
var __app_insights_name = replace(
  '${deploymentParams.enterprise_name_suffix}-${fn_params.name_prefix}-${deploymentParams.loc_short_code}-app-insights-${deploymentParams.global_uniqueness}',
  '_',
  '-'
)
resource r_app_insights 'Microsoft.Insights/components@2020-02-02' = {
  name: __app_insights_name
  location: deploymentParams.location
  tags: tags
  kind: 'web'
  properties: {
    Application_Type: 'web'
    Request_Source: 'rest'
    WorkspaceResourceId: laws_id
    publicNetworkAccessForIngestion: 'Enabled'
    publicNetworkAccessForQuery: 'Enabled'
  }
}

@description('Create Function App Hosting Plan')
var __fn_svc_name = replace(
  '${deploymentParams.enterprise_name_suffix}-${fn_params.name_prefix}-${deploymentParams.loc_short_code}-fn-plan-${deploymentParams.global_uniqueness}',
  '_',
  '-'
)

resource r_fn_hosting_plan 'Microsoft.Web/serverfarms@2022-03-01' = {
  name: __fn_svc_name
  location: deploymentParams.location
  tags: tags
  kind: 'linux'
  sku: {
    // https://learn.microsoft.com/en-us/azure/azure-resource-manager/resource-manager-sku-not-available-errors
    name: 'Y1'
    tier: 'Dynamic'
    family: 'Y'
  }
  properties: {
    reserved: true
  }
}

@description('Create Function App')
var __fn_app_name = replace(
  '${deploymentParams.enterprise_name_suffix}-${fn_params.name_prefix}-${deploymentParams.loc_short_code}-fn-app-${deploymentParams.global_uniqueness}',
  '_',
  '-'
)

resource r_fn_app 'Microsoft.Web/sites@2021-03-01' = {
  name: __fn_app_name
  location: deploymentParams.location
  kind: 'functionapp,linux'
  tags: tags
  identity: {
    // type: 'SystemAssigned'
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${r_uami_func.id}': {}
    }
  }
  properties: {
    enabled: true
    reserved: true
    serverFarmId: r_fn_hosting_plan.id
    clientAffinityEnabled: false
    clientCertEnabled: false
    // clientCertMode: 'Required'
    // redundancyMode: 'GeoRedundant'
    httpsOnly: true
    siteConfig: {
      linuxFxVersion: 'Python|3.11' //az webapp list-runtimes --linux || az functionapp list-runtimes --os linux -o table
      // appCommandLine: 'python -m streamlit run Home.py --server.port 8000 --server.address 0.0.0.0'
      // ftpsState: 'FtpsOnly'
      ftpsState: 'Disabled'
      minTlsVersion: '1.2'
      cors: {
        allowedOrigins: ['https://portal.azure.com', 'https://ms.portal.azure.com']
        supportCredentials: false
      }
    }
  }
  dependsOn: [
    r_app_insights
  ]
}

resource r_fn_app_settings 'Microsoft.Web/sites/config@2021-03-01' = {
  parent: r_fn_app
  name: 'appsettings' // Reservered Name
  properties: {
    FUNCTION_APP_EDIT_MODE: 'readwrite'
    FUNCTIONS_EXTENSION_VERSION: '~4'
    WEBSITE_FUNCTIONS_ARMCACHE_ENABLED: '0'

    FUNCTIONS_WORKER_RUNTIME: 'python'
    PYTHON_THREADPOOL_THREAD_COUNT: '20'

    //https://learn.microsoft.com/en-us/azure/azure-monitor/app/statsbeat?tabs=eu-java%2Cpython#configure-statsbeat
    APPLICATIONINSIGHTS_STATSBEAT_DISABLED_ALL: 'true'
    APPLICATIONINSIGHTS_CONNECTION_STRING: r_app_insights.properties.ConnectionString
    AzureWebJobsStorage: 'DefaultEndpointsProtocol=https;AccountName=${misc_sa_name};EndpointSuffix=${environment().suffixes.storage};AccountKey=${r_misc_sa_ref.listKeys().keys[0].value}'

    AzureWebJobsFeatureFlags: 'EnableWorkerIndexing'

    // https://learn.microsoft.com/en-us/azure/azure-functions/configure-monitoring?tabs=v2#overriding-monitoring-configuration-at-runtime
    AzureFunctionsJobHost__logging__LogLevel__Default: 'Information'

    // ENABLE_ORYX_BUILD: 'true'
    // SCM_DO_BUILD_DURING_DEPLOYMENT: 'true'

    WAREHOUSE_STORAGE: 'DefaultEndpointsProtocol=https;AccountName=${sa_name};EndpointSuffix=${environment().suffixes.storage};AccountKey=${r_sa_ref.listKeys().keys[0].value}'
    WAREHOUSE_STORAGE_CONTAINER: blob_container_name

    // SETTINGS FOR MANAGED IDENTITY AUTHENTICAION
    // https://github.com/microsoft/azure-container-apps/issues/442#issuecomment-1272352665
    AZURE_CLIENT_ID: r_uami_func.properties.clientId

    // SETTINGS FOR STORAGE ACCOUNT
    SA_CONNECTION__accountName: sa_name
    SA_CONNECTION__clientId: r_uami_func.properties.clientId
    SA_CONNECTION__credential: 'managedidentity'
    SA_CONNECTION__serviceUri: 'https://${sa_name}.blob.${environment().suffixes.storage}'
    SA_CONNECTION__blobServiceUri: 'https://${sa_name}.blob.${environment().suffixes.storage}' // Producer - https://warehousejwnff5001.blob.core.windows.net
    // SA_CONNECTION__queueServiceUri: 'https://${sa_name}.queue.${environment().suffixes.storage}'
    BLOB_SVC_ACCOUNT_URL: r_sa_ref.properties.primaryEndpoints.blob
    SA_NAME: r_sa_ref.name
    BLOB_NAME: blob_container_name

    // FUNCTION UAMI CREDENTIAL SETTINGS
    UAMI_CONNECTION__credential: 'managedidentity'
    UAMI_CONNECTION__clientId: r_uami_func.properties.clientId
    UAMI_CONNECTION__tenantId: r_uami_func.properties.tenantId

    // SETTINGS FOR SERVICE BUS
    SVC_BUS_CONNECTION__fullyQualifiedNamespace: '${svc_bus_ns_name}.servicebus.windows.net'
    SVC_BUS_CONNECTION__credential: 'managedidentity'
    SVC_BUS_CONNECTION__clientId: r_uami_func.properties.clientId

    SVC_BUS_FQDN: '${svc_bus_ns_name}.servicebus.windows.net'
    SVC_BUS_Q_NAME: svc_bus_q_name
    SVC_BUS_TOPIC_NAME: svc_bus_topic_name
    SALES_EVENTS_SUBSCRIPTION_NAME: sales_events_subscriber_name
    ALL_EVENTS_SUBSCRIPTION_NAME: all_events_subscriber_name

    // SETTINGS FOR COSMOS DB
    COSMOS_DB_CONNECTION__accountEndpoint: r_cosmos_db_accnt.properties.documentEndpoint
    COSMOS_DB_CONNECTION__credential: 'managedidentity'
    COSMOS_DB_CONNECTION__clientId: r_uami_func.properties.clientId

    COSMOS_DB_URL: r_cosmos_db_accnt.properties.documentEndpoint
    COSMOS_DB_NAME: cosmos_db_name
    COSMOS_DB_CONTAINER_NAME: cosmos_db_container_name

    // EVENT HUB CONNECTION SETTINGS
    // EVENT_HUB_CONNECTION__fullyQualifiedNamespace: '${event_hub_ns_name}.servicebus.windows.net'
    // EVENT_HUB_CONNECTION__credential: 'managedidentity'
    // EVENT_HUB_CONNECTION__clientId: r_uami_func.properties.clientId
    // EVENT_HUB_FQDN: '${event_hub_ns_name}.servicebus.windows.net'
    // EVENT_HUB_NAME: event_hub_name
    // EVENT_HUB_SALE_EVENTS_CONSUMER_GROUP_NAME: event_hub_sale_events_consumer_group_name
  }
  dependsOn: [
    r_sa_ref
    r_misc_sa_ref
  ]
}

// Function App Binding
resource r_fn_app_binding 'Microsoft.Web/sites/hostNameBindings@2022-03-01' = {
  parent: r_fn_app
  name: '${r_fn_app.name}.azurewebsites.net'
  properties: {
    siteName: r_fn_app.name
    hostNameType: 'Verified'
  }
}

resource r_fn_app_logs 'Microsoft.Web/sites/config@2022-09-01' = {
  parent: r_fn_app
  name: 'logs' // Hard Coded Name - https://learn.microsoft.com/en-us/azure/templates/microsoft.web/sites/config-logs
  properties: {
    applicationLogs: {
      azureBlobStorage: {
        level: 'Information'
        retentionInDays: 10
      }
    }
    httpLogs: {
      fileSystem: {
        retentionInMb: 100
        enabled: true
      }
    }
    detailedErrorMessages: {
      enabled: true
    }
    failedRequestsTracing: {
      enabled: true
    }
  }
  dependsOn: [
    r_fn_app_settings
  ]
}

@description('No execuition in 5mins alert')
resource r_fn_app_no_exec_alert 'Microsoft.Insights/metricAlerts@2018-03-01' = {
  name: '${__fn_app_name}__no_exec_in_5min_alert'
  location: 'global'
  tags: tags
  properties: {
    description: 'Alert when No execuition in the last 5min'
    severity: 0
    enabled: true
    autoMitigate: true
    targetResourceRegion: deploymentParams.location
    targetResourceType: 'Microsoft.Web/sites'
    scopes: [
      r_fn_app.id
    ]
    evaluationFrequency: 'PT1M'
    windowSize: 'PT5M'
    criteria: {
      allOf: [
        {
          criterionType: 'StaticThresholdCriterion'
          name: 'fn_no_exec_in_5mins_metric1'
          metricNamespace: 'Microsoft.Web/sites'
          metricName: 'FunctionExecutionCount'
          // dimensions: [
          //   {
          //     name: 'Location'
          //     operator: 'Include'
          //     values: [
          //       '*'
          //     ]
          //   }
          // ]
          timeAggregation: 'Total'
          operator: 'LessThanOrEqual'
          threshold: 1
          skipMetricValidation: false
        }
      ]
      'odata.type': 'Microsoft.Azure.Monitor.SingleResourceMultipleMetricCriteria'
    }
    actions: [
      {
        actionGroupId: alert_action_group_id
        webHookProperties: {}
      }
    ]
  }
}

////////////////////////////////////////////
//                                        //
//         Diagnostic Settings            //
//                                        //
////////////////////////////////////////////

@description('Diagnostic Settings for Function App')
resource r_fn_app_diag 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = {
  name: '${fn_params.name_prefix}-diags-${deploymentParams.global_uniqueness}'
  scope: r_fn_app
  properties: {
    workspaceId: laws_id
    logs: [
      {
        category: 'FunctionAppLogs'
        enabled: true
      }
    ]
    metrics: [
      {
        category: 'AllMetrics'
        enabled: true
      }
    ]
    logAnalyticsDestinationType: 'Dedicated'
  }
}

// OUTPUTS
output module_metadata object = module_metadata

//FunctionApp Outputs
output fn_app_name string = r_fn_app.name

output r_app_insights_name string = r_app_insights.name

// Function Outputs
output fn_app_url string = r_fn_app.properties.defaultHostName
output fn_url string = 'https://${r_fn_app.properties.defaultHostName}'
