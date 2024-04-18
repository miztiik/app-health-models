// SET MODULE DATE
param module_metadata object = {
  module_last_updated: '2024-04-13'
  owner: 'miztiik@github'
}

param deploymentParams object
param svc_bus_params object
param tags object
param alert_action_group_id string

param enableDiagnostics bool = true
param logAnalyticsWorkspaceId string

var svc_bus_name = replace(
  '${deploymentParams.enterprise_name_suffix}-${deploymentParams.loc_short_code}-${svc_bus_params.name_prefix}-svc-bus-ns-${deploymentParams.global_uniqueness}',
  '_',
  '-'
)

resource r_svc_bus_ns 'Microsoft.ServiceBus/namespaces@2022-01-01-preview' = {
  name: svc_bus_name
  location: deploymentParams.location
  tags: tags
  sku: {
    name: 'Standard'
    //name: 'Premium'
  }
  properties: {}
}

var svc_bus_q_name = replace(
  '${svc_bus_params.name_prefix}-${deploymentParams.loc_short_code}-q-${deploymentParams.enterprise_name_suffix}-${deploymentParams.global_uniqueness}',
  '_',
  '-'
)

resource r_svc_bus_q 'Microsoft.ServiceBus/namespaces/queues@2022-01-01-preview' = {
  parent: r_svc_bus_ns
  name: svc_bus_q_name
  properties: {
    lockDuration: 'PT5M'
    maxSizeInMegabytes: 1024
    requiresDuplicateDetection: false
    requiresSession: false
    defaultMessageTimeToLive: 'P7D'
    deadLetteringOnMessageExpiration: false
    duplicateDetectionHistoryTimeWindow: 'PT10M'
    maxDeliveryCount: 5
    // autoDeleteOnIdle: 'P10675199DT2H48M5.4775807S'
    enablePartitioning: false
    enableExpress: false
  }
}

// Create Alert for DLQ
resource r_q_dlq_gt_5_alert 'Microsoft.Insights/metricAlerts@2018-03-01' = {
  name: '${svc_bus_params.name_prefix}_${deploymentParams.global_uniqueness}__q_dlq_gt_5_alert'
  location: 'global'
  tags: tags
  properties: {
    description: 'Alert when Queue DLQ has >5 messages in last 5 minutes '
    severity: 0
    enabled: true
    autoMitigate: true
    targetResourceRegion: deploymentParams.location
    targetResourceType: 'Microsoft.ServiceBus/namespaces'
    scopes: [
      r_svc_bus_ns.id
    ]
    evaluationFrequency: 'PT1M'
    windowSize: 'PT5M'
    criteria: {
      allOf: [
        {
          criterionType: 'StaticThresholdCriterion'
          name: 'dlq_gt_5_in_5_min_Metric1'
          metricNamespace: 'Microsoft.ServiceBus/namespaces'
          metricName: 'DeadletteredMessages'
          dimensions: [
            {
              name: 'EntityName'
              operator: 'Include'
              values: [
                svc_bus_q_name
              ]
            }
          ]
          timeAggregation: 'Minimum'
          operator: 'GreaterThanOrEqual'
          threshold: 4
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

// Alert for Service Bus Throttled Requests
resource r_q_throttle_reqs_alert 'Microsoft.Insights/metricAlerts@2018-03-01' = {
  name: '${svc_bus_params.name_prefix}_${deploymentParams.global_uniqueness}__q_throttle_reqs_alert'
  location: 'global'
  tags: tags
  properties: {
    description: 'Alert when a Service Bus q entity has throttled more than 5 requests.'
    evaluationFrequency: 'PT1M'
    windowSize: 'PT5M'
    severity: 2
    enabled: true
    criteria: {
      'odata.type': 'Microsoft.Azure.Monitor.SingleResourceMultipleMetricCriteria'
      allOf: [
        {
          metricName: 'ThrottledRequests'
          metricNamespace: 'Microsoft.ServiceBus/namespaces'
          name: 'Metric1'
          skipMetricValidation: false
          timeAggregation: 'Total'
          criterionType: 'StaticThresholdCriterion'
          operator: 'GreaterThan'
          threshold: 10
          dimensions: [
            {
              name: 'EntityName'
              operator: 'Include'
              values: [
                svc_bus_q_name
              ]
            }
          ]
        }
      ]
    }
    scopes: [r_svc_bus_ns.id]
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

@description('Enabling Diagnostics for the Service Bus Namespace')
resource r_svc_bus_ns_diags 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' =
  if (enableDiagnostics) {
    name: '${svc_bus_name}-diags'
    scope: r_svc_bus_ns
    properties: {
      workspaceId: logAnalyticsWorkspaceId
      logs: [
        {
          categoryGroup: 'allLogs'
          enabled: true
        }
        {
          categoryGroup: 'audit'
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

output svc_bus_ns_name string = r_svc_bus_ns.name
output svc_bus_q_name string = r_svc_bus_q.name
