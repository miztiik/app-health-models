// SET MODULE DATE
param module_metadata object = {
  module_last_updated: '2024-04-22'
  owner: 'miztiik@github'
}

param deploymentParams object
param laws_params object
param tags object

@description('Create the LogAnalytics Workspace - Pay-As-You-Go Tier')
var laws_name = replace(
  '${deploymentParams.enterprise_name_suffix}-${deploymentParams.loc_short_code}-${laws_params.name_prefix}-laws-${deploymentParams.global_uniqueness}',
  '_',
  '-'
)

resource r_laws 'Microsoft.OperationalInsights/workspaces@2022-10-01' = {
  name: laws_name
  location: deploymentParams.location
  tags: tags
  properties: {
    retentionInDays: 32
    sku: {
      name: 'PerGB2018'
    }
    workspaceCapping: {
      dailyQuotaGb: 20
    }
    publicNetworkAccessForIngestion: 'Enabled'
    publicNetworkAccessForQuery: 'Enabled'
  }
}

// az monitor log-analytics workspace table list --resource-group Miztiik_Enterprises_Log_Monitor_002 --workspace-name lumberyard-payGTier-002
resource r_storeEventsCustomTable 'Microsoft.OperationalInsights/workspaces/tables@2022-10-01' = {
  parent: r_laws
  name: '${laws_params.store_events_custom_tbl_name}${deploymentParams.global_uniqueness}_CL'
  // tags: tags
  properties: {
    // plan: 'Basic'
    /*
    Apparently Basic plan does not support custom tables, ARM throws an error. Couldn't find the actual doc sayin it
    https://learn.microsoft.com/en-us/azure/azure-monitor/logs/basic-logs-configure?tabs=portal-1
    */
    plan: 'Analytics'
    retentionInDays: 4
    schema: {
      description: 'Store order events custom table'
      displayName: 'DOESNT-SEEM-TO-WORK-STORE-EVENTS-0'
      name: '${laws_params.store_events_custom_tbl_name}${deploymentParams.global_uniqueness}_CL'
      columns: [
        {
          name: 'TimeGenerated'
          type: 'datetime'
        }
        {
          name: 'RawData'
          type: 'string'
        }
        {
          name: 'request_id'
          type: 'string'
        }
        {
          name: 'event_type'
          type: 'string'
        }
        {
          name: 'store_id'
          displayName: 'store_id'
          description: 'The Id of the store placing the Order'
          type: 'int'
        }
        {
          name: 'cust_id'
          type: 'int'
        }
        {
          name: 'category'
          type: 'string'
        }
        {
          name: 'sku'
          type: 'int'
        }
        {
          name: 'price'
          type: 'real'
        }
        {
          name: 'qty'
          type: 'int'
        }
        {
          name: 'discount'
          type: 'real'
        }
        {
          name: 'gift_wrap'
          type: 'boolean'
        }
        {
          name: 'variant'
          description: 'Product Variety'
          type: 'string'
        }
        {
          name: 'priority_shipping'
          description: 'Priority Shipping requested'
          type: 'boolean'
        }
        {
          name: 'contact_me'
          description: 'Miztiik Automation Brand Experience Store'
          displayName: 'contact_me'
          type: 'string'
        }
      ]
    }
  }
}

resource r_automationEventsCustomTable 'Microsoft.OperationalInsights/workspaces/tables@2022-10-01' = {
  parent: r_laws
  name: '${laws_params.automationEventsCustomTableName}${deploymentParams.global_uniqueness}_CL'
  properties: {
    // plan: 'Basic'
    /*
    Apparently Basic plan does not support custom tables, ARM throws an error. Couldn't find the actual doc sayin it
    https://learn.microsoft.com/en-us/azure/azure-monitor/logs/basic-logs-configure?tabs=portal-1
    */
    plan: 'Analytics'
    retentionInDays: 4
    schema: {
      description: 'Miztiik Automation Events'
      displayName: 'DOESNT-SEEM-TO-WORK-AUTOMATION-EVENTS-1'
      name: '${laws_params.automationEventsCustomTableName}${deploymentParams.global_uniqueness}_CL'
      columns: [
        {
          name: 'TimeGenerated'
          type: 'datetime'
        }
        {
          name: 'RawData'
          type: 'string'
        }
        {
          name: 'request_id'
          type: 'string'
        }
      ]
    }
  }
}

resource r_managed_run_cmd_CustomTable 'Microsoft.OperationalInsights/workspaces/tables@2022-10-01' = {
  parent: r_laws
  name: '${laws_params.managedRunCmdCustomTableName}${deploymentParams.global_uniqueness}_CL'
  properties: {
    // plan: 'Basic'
    /*
    Apparently Basic plan does not support custom tables, ARM throws an error. Couldn't find the actual doc sayin it
    https://learn.microsoft.com/en-us/azure/azure-monitor/logs/basic-logs-configure?tabs=portal-1
    */
    plan: 'Analytics'
    retentionInDays: 4
    schema: {
      description: 'Miztiik Run Command Automation Events'
      displayName: 'DOESNT-SEEM-TO-WORK-AUTOMATION-EVENTS-2'
      name: '${laws_params.managedRunCmdCustomTableName}${deploymentParams.global_uniqueness}_CL'
      columns: [
        {
          name: 'TimeGenerated'
          type: 'datetime'
        }
        {
          name: 'RawData'
          type: 'string'
        }
      ]
    }
  }
}

////////////////////////////////////////////
//                                        //
//         Diagnostic Settings            //
//                                        //
////////////////////////////////////////////

@description('Create diagnostic settings for Log Analytics Workspace')
resource r_logAnalyticsPayGWorkspace_diag 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = {
  name: '${laws_name}-diags-${deploymentParams.global_uniqueness}'
  scope: r_laws
  properties: {
    workspaceId: r_laws.id
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
        category: 'AllMetrics'
        enabled: true
      }
    ]
  }
}

// OUTPUTS
output module_metadata object = module_metadata

output logAnalyticsPayGWorkspaceId string = r_laws.id
output logAnalyticsPayGWorkspaceName string = r_laws.name

output storeEventsCustomTableNamePrefix string = '${laws_params.store_events_custom_tbl_name}${deploymentParams.global_uniqueness}'
output storeEventsCustomTableName string = r_storeEventsCustomTable.name

output automationEventsCustomTableNamePrefix string = '${laws_params.automationEventsCustomTableName}${deploymentParams.global_uniqueness}'
output automationEventsCustomTableName string = r_automationEventsCustomTable.name

output managedRunCmdCustomTableNamePrefix string = '${laws_params.managedRunCmdCustomTableName}${deploymentParams.global_uniqueness}'
output managedRunCmdCustomTableName string = r_managed_run_cmd_CustomTable.name
