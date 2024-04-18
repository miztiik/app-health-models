// SET MODULE DATE
param module_metadata object = {
  module_last_updated: '2024-04-13'
  owner: 'miztiik@github'
}

param deploymentParams object
param tags object

param alert_name string
param alert_action_group_id string
param alert_display_name string
param alert_description string

// param scope_workspaceId_1 string // log analytics workspace resource id
// param alertRuleSeverity int
param alert_window_size string
param alert_eval_frequency string
// param autoMitigate bool
// param kql_alert_query string

var __alert_name = replace(
  '${deploymentParams.enterprise_name_suffix}-${deploymentParams.loc_short_code}-${deploymentParams.global_uniqueness}-${alert_name}-alert',
  '_',
  '-'
)

// Create Alert for DLQ
resource r_alert 'Microsoft.Insights/metricAlerts@2018-03-01' = {
  name: __alert_name
  location: 'global'
  tags: tags
  properties: {
    displayName: alert_display_name
    description: alert_description
    severity: 0
    enabled: true
    autoMitigate: true
    targetResourceRegion: deploymentParams.location
    targetResourceType: 'Microsoft.ServiceBus/namespaces'
    scopes: [
      r_svc_bus_ns.id
    ]
    evaluationFrequency: alert_eval_frequency
    windowSize: alert_window_size
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

// resource rule 'Microsoft.Insights/scheduledQueryRules@2022-08-01-preview' = {
//   location: deploymentParams.location
//   tags: tags
//   name: alertRuleName
//   properties: {
//     description: alertRuleDescription
//     displayName: alertRuleDisplayName
//     enabled: true
//     scopes: [
//       scope_workspaceId_1
//     ]
//     targetResourceTypes: [
//       'Microsoft.OperationalInsights/workspaces'
//     ]
//     windowSize: windowSize
//     evaluationFrequency: evaluationFrequency
//     severity: alertRuleSeverity
//     autoMitigate: autoMitigate
//     criteria: {
//       allOf: [
//           {
//               query: kql_alert_query
//               timeAggregation: 'Count'
//               dimensions: []
//               operator: 'GreaterThan'
//               threshold: 1
//               failingPeriods: {
//                   numberOfEvaluationPeriods: 1
//                   minFailingPeriodsToAlert: 1
//               }
//           }
//       ]
//     }

//   }
// }

// OUTPUTS
output module_metadata object = module_metadata
