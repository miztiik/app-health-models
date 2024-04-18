// SET MODULE DATE
param module_metadata object = {
  module_last_updated: '2024-04-13'
  owner: 'miztiik@github'
}

param deploymentParams object
param tags object

var __action_grp_name = replace(
  '${deploymentParams.enterprise_name_suffix}-${deploymentParams.loc_short_code}-EmailActionGroup-${deploymentParams.global_uniqueness}',
  '_',
  '-'
)

// Email Action Group
resource r_email_on_alerts 'Microsoft.Insights/actionGroups@2023-01-01' = {
  name: __action_grp_name
  location: 'Global'
  tags: tags
  properties: {
    groupShortName: 'Email'
    enabled: true
    emailReceivers: [
      {
        name: 'Myztique'
        emailAddress: 'abc@xyz.com'
      }
    ]
  }
}

// OUTPUTS
output module_metadata object = module_metadata

output alert_action_group_id string = r_email_on_alerts.id
