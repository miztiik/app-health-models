// SET MODULE DATE
param module_metadata object = {
  module_last_updated: '2024-04-13'
  owner: 'miztiik@github'
}

param deploymentParams object
param tags object
param logAnalyticsWorkspaceId string
param alert_action_group_id string

param fn_app_name string
param app_insights_name string
param event_generator_fn_name string

var enable_diagnostics = deploymentParams.enable_diagnostics

@description('Get App Insights Workspace Id')
resource r_app_insights_Ref 'Microsoft.Insights/components@2020-02-02' existing = {
  name: app_insights_name
}

resource r_fn_app_Ref 'Microsoft.Web/sites@2022-03-01' existing = {
  name: fn_app_name
}

@description('Create API Management Service')
var __apim_name = replace(
  '${deploymentParams.enterprise_name_suffix}-store-front-${deploymentParams.loc_short_code}-apim-${deploymentParams.global_uniqueness}',
  '_',
  '-'
)

resource r_apim_1 'Microsoft.ApiManagement/service@2022-08-01' = {
  name: __apim_name
  location: deploymentParams.location
  tags: tags
  sku: {
    name: 'Consumption' // Developer, Basic, Standard, Premium, Consumption
    capacity: 0 // has to be 0 for Consumption tier
  }
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    publisherEmail: 'Miztiik@github'
    publisherName: 'Mystique Corp'
    publicNetworkAccess: 'Enabled'
    // certificates:
  }
}

@description('Create logger for APIM')
resource r_apim_1_logger 'Microsoft.ApiManagement/service/loggers@2022-09-01-preview' = {
  name: '${__apim_name}_logger'
  parent: r_apim_1
  properties: {
    loggerType: 'applicationInsights'
    resourceId: r_app_insights_Ref.id
    credentials: {
      instrumentationKey: r_app_insights_Ref.properties.InstrumentationKey
    }
    description: 'Application Insights telemetry from APIs'
  }
}

@description('Create Tags for APIs')
resource r_apim_1_tags 'Microsoft.ApiManagement/service/tags@2023-03-01-preview' = {
  name: '${__apim_name}_tags'
  parent: r_apim_1
  properties: {
    displayName: 'miztiik-store-front'
  }
}

@description('Create Named Values for APIM')
resource r_apim_1_named_value_apim_name 'Microsoft.ApiManagement/service/namedValues@2021-08-01' = {
  name: 'API_M_NAME'
  parent: r_apim_1
  properties: {
    displayName: 'API_M_NAME'
    secret: false
    value: r_apim_1.name
  }
}

@description('Create Sample Policy for APIM')
var __apim_policy_str = '<!--\r\n    MIZTIIK SAYS IT IS IMPORTANT:\r\n    - Policy elements can appear only within the <inbound>, <outbound>, <backend> section elements.\r\n    - Only the <forward-request> policy element can appear within the <backend> section element.\r\n    - To apply a policy to the incoming request (before it is forwarded to the backend service), place a corresponding policy element within the <inbound> section element.\r\n    - To apply a policy to the outgoing response (before it is sent back to the caller), place a corresponding policy element within the <outbound> section element.\r\n    - To add a policy position the cursor at the desired insertion point and click on the round button associated with the policy.\r\n    - To remove a policy, delete the corresponding policy statement from the policy document.\r\n    - Policies are applied in the order of their appearance, from the top down.\r\n-->\r\n<policies>\r\n  <inbound></inbound>\r\n  <backend>\r\n    <forward-request />\r\n  </backend>\r\n  <outbound></outbound>\r\n</policies>'
resource r_apim_1_policy 'Microsoft.ApiManagement/service/policies@2021-08-01' = {
  name: 'policy'
  parent: r_apim_1
  properties: {
    format: 'xml'
    value: __apim_policy_str
  }
}

////////////////////////////////////////////
//                                        //
//        BACKEND DEFINITION              //
//                                        //
////////////////////////////////////////////

@description('Create Named Values for Function Key')
resource r_apim_1_named_value_fn_key 'Microsoft.ApiManagement/service/namedValues@2021-12-01-preview' = {
  parent: r_apim_1
  name: 'fn-api-key'
  properties: {
    displayName: 'fn-api-key'
    secret: true
    value: listkeys('${r_fn_app_Ref.id}/host/default', '2016-08-01').functionKeys.default
    tags: [
      'miztiik_automation'
      'key'
      'function'
      'auto'
    ]
  }
}

var __backend_name = 'generate-events'

resource r_apim_1_backend_event_generator 'Microsoft.ApiManagement/service/backends@2021-12-01-preview' = {
  parent: r_apim_1
  name: __backend_name
  properties: {
    title: 'WHERE DOES THE TITLE GO?' // Shows up in the resource blade
    description: 'Azure Function that generates store event(s) and send it to the service bus topic'
    // url: 'https://${r_fn_app_Ref.name}.azurewebsites.net'
    url: 'https://${r_fn_app_Ref.properties.hostNames[0]}'
    protocol: 'http'
    resourceId: '${environment().resourceManager}${r_fn_app_Ref.id}'
    credentials: {
      query: {}
      header: {
        'x-functions-key': [
          '{{${r_apim_1_named_value_fn_key.name}}}'
        ]
      }
    }

    tls: {
      validateCertificateChain: false
      validateCertificateName: false
    }
  }
}

////////////////////////////////////////////
//                                        //
//            API DEFINITION              //
//                                        //
////////////////////////////////////////////

@description('Resource definition for API within Azure API Management')
resource r_apim_1_apis 'Microsoft.ApiManagement/service/apis@2022-08-01' = {
  parent: r_apim_1
  name: 'store-events-api'
  properties: {
    displayName: 'Store Events API'
    path: 'api'
    description: 'API for miztiik store front'
    protocols: [
      'https'
    ]
    isCurrent: true
    // apiRevision: '1'
    // apiVersion: '1'
    // subscriptionKeyParameterNames: {
    //   header: 'Ocp-Apim-Subscription-Key'
    //   query: 'subscription-key'
    // }
    subscriptionRequired: false
  }
}

@description('Resource definition for an "event generation" operation within the API')
resource r_apim_1_apis_get_event 'Microsoft.ApiManagement/service/apis/operations@2022-08-01' = {
  parent: r_apim_1_apis
  name: 'generate-events'
  properties: {
    displayName: 'Generate Store Event'
    method: 'GET'
    urlTemplate: '/${event_generator_fn_name}' //HARD CODED FOR NOW - TODO: FIX THIS // As my functionapp has two functions getting the name is going to be hard.
    request: {
      description: 'Generate store event(s) and send it to the service bus topic'
      queryParameters: [
        {
          name: 'count'
          type: 'integer'
          required: false
        }
      ]
      headers: [] // Empty headers - No API Key Required
    }
    responses: [
      // {
      //   statusCode: 200
      //   description: 'Store Event Generated Successfully'
      // }
      {
        description: 'not authorized'
        statusCode: 401
      }
      {
        description: 'Miztiik NO'
        statusCode: 404
      }
    ]
  }
}

@description('Add APIM policy to API')
var __policy_content = loadTextContent('api_policies/fn_backend_policy.xml')
var __fn_backend_policy = replace(__policy_content, '__BACKEND-ID__', '${r_apim_1_backend_event_generator.name}')
resource r_apim_1_apis_policy 'Microsoft.ApiManagement/service/apis/operations/policies@2021-12-01-preview' = {
  name: 'policy'
  parent: r_apim_1_apis_get_event
  properties: {
    // value: replace(replace(loadTextContent('apimPolicies/operation.xml'), '{method}', 'GET'), '{template}', '/orders/{storeId}')
    value: __fn_backend_policy
    format: 'rawxml'
  }
}

@description('Adding Tags to API')
resource r_apim_1_apis_tags 'Microsoft.ApiManagement/service/tags/apiLinks@2023-03-01-preview' = {
  name: 'string'
  parent: r_apim_1_tags
  properties: {
    apiId: r_apim_1_apis.id
  }
}

@description('Create general purpose logger for APIs')
resource r_apim_1_apis_logger 'Microsoft.ApiManagement/service/apis/diagnostics@2022-09-01-preview' = {
  name: 'applicationinsights' //TODO: Apparently this is a reserved name.
  parent: r_apim_1_apis
  properties: {
    alwaysLog: 'allErrors'
    logClientIp: true
    metrics: true
    httpCorrelationProtocol: 'W3C'
    verbosity: 'verbose'
    loggerId: r_apim_1_logger.id
    sampling: {
      samplingType: 'fixed'
      percentage: 100
    }
    frontend: {
      request: {
        body: {
          bytes: 1024
        }
      }
    }
    backend: {
      // request: {
      //   body: {
      //     bytes: 1024
      //   }
      // }
      response: {
        body: {
          bytes: 1024
        }
      }
    }
  }
}

@description('Create Slow Response alert for APIM')
resource r_apim_avg_resp_gt_23s_alert 'Microsoft.Insights/metricAlerts@2018-03-01' = {
  name: '${__apim_name}__avg_resp_gt_23s_alert'
  location: 'global'
  tags: tags
  properties: {
    description: 'Alert when APIM average response is >23 seconds in the last 5'
    severity: 0
    enabled: true
    autoMitigate: true
    targetResourceRegion: deploymentParams.location
    targetResourceType: 'Microsoft.ApiManagement/service'
    scopes: [
      r_apim_1.id
    ]
    evaluationFrequency: 'PT1M'
    windowSize: 'PT5M'
    criteria: {
      allOf: [
        {
          criterionType: 'StaticThresholdCriterion'
          name: 'apim_avg_resp_gt_23s_metric1'
          metricNamespace: 'microsoft.apimanagement/service'
          metricName: 'Duration'
          dimensions: [
            {
              name: 'Location'
              operator: 'Include'
              values: [
                '*'
              ]
            }
          ]
          timeAggregation: 'Maximum'
          operator: 'GreaterThanOrEqual'
          threshold: 21000
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

@description('Diagnostic Settings for APIM')
resource r_apim_svc_diag 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' =
  if (enable_diagnostics) {
    name: '${__apim_name}-diags'
    scope: r_apim_1
    properties: {
      workspaceId: logAnalyticsWorkspaceId
      logs: [
        {
          categoryGroup: 'allLogs'
          enabled: true
        }
        // {
        //   categoryGroup: 'audit'
        //   enabled: true
        // }
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

//APIM Outputs
output apim_name string = r_apim_1.name

output apim_hostname string = r_apim_1.properties.gatewayUrl
output producer_api_url string = '${r_apim_1.properties.gatewayUrl}/${r_apim_1_apis.properties.path}${r_apim_1_apis_get_event.properties.urlTemplate}'
