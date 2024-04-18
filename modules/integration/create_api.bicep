// SET MODULE DATE
param module_metadata object = {
  module_last_updated: '2023-12-31'
  owner: 'miztiik@github'
}

param deploymentParams object
param tags object
param logAnalyticsWorkspaceId string

param __apim_name string
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
// // var __apim_name = replace('${deploymentParams.enterprise_name_suffix}-${fn_params.app_name_prefix}-${deploymentParams.loc_short_code}-apim-${deploymentParams.global_uniqueness}', '_', '-')
// var __apim_name = replace('${deploymentParams.enterprise_name_suffix}-store-front-${deploymentParams.loc_short_code}-apim-${deploymentParams.global_uniqueness}', '_', '-')

resource r_apim_ref 'Microsoft.ApiManagement/service@2022-08-01' existing = {
  name: __apim_name
}

////////////////////////////////////////////
//                                        //
//            API DEFINITION              //
//                                        //
////////////////////////////////////////////

@description('Resource definition for API within Azure API Management')
resource r_apim_1_apis 'Microsoft.ApiManagement/service/apis@2022-08-01' = {
  parent: r_apim_ref
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
        // {
        //   name: 'filter'
        //   type: 'string'
        //   required: false
        // }
      ]
      headers: [] // Empty headers - No API Key Required
    }
    responses: [
      // {
      //   statusCode: 200
      //   description: 'Store Event Generated Successfully'
      // }
      // {
      //   description: 'not authorized'
      //   statusCode: 401
      // }
    ]
  }
}

var __policy_content = loadTextContent('api_policies/fn_backend_policy.xml', 'utf-8')
var __fn_backend_policy = replace(__policy_content, '__BACKEND-ID__', '${r_apim_1_backend_event_generator.name}')
resource getOrdersPolicy 'Microsoft.ApiManagement/service/apis/operations/policies@2021-12-01-preview' = {
  name: 'policy'
  parent: r_apim_1_apis_get_event
  properties: {
    // value: replace(replace(loadTextContent('apimPolicies/operation.xml'), '{method}', 'GET'), '{template}', '/orders/{storeId}')
    value: __fn_backend_policy
    format: 'xml'
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
    backend: {
      response: {
        body: {
          bytes: 1024
        }
      }
    }
  }
}

////////////////////////////////////////////
//                                        //
//         Diagnostic Settings            //
//                                        //
////////////////////////////////////////////

@description('Diagnostic Settings for APIM')
resource r_apim_svc_diag 'Microsoft.Insights/diagnosticSettings@2021-05-01-preview' = if (enable_diagnostics) {
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
output fn_name_tst string = r_fn_app_Ref.properties.usageState
output svc_url string = '${r_apim_1.properties.gatewayUrl}/api/generate-events'
