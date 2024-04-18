// SET MODULE DATE
param module_metadata object = {
  module_last_updated: '2023-12-31'
  owner: 'miztiik@github'
}

param deploymentParams object
param vnet_params object

param tags object = resourceGroup().tags

param vnet_address_prefixes object = {
  addressPrefixes: [
    '10.0.0.0/16'
  ]
}

var subnet_cidrs = [
  {
    name: 'web_subnet_01'
    subnet_prefix: '10.0.0.0/24'
  }
  {
    name: 'web_subnet_02'
    subnet_prefix: '10.0.1.0/24'
  }
  {
    name: 'app_subnet_01'
    subnet_prefix: '10.0.2.0/24'
  }
  {
    name: 'app_subnet_02'
    subnet_prefix: '10.0.3.0/24'
  }
  {
    name: 'db_subnet_01'
    subnet_prefix: '10.0.4.0/24'
  }
  {
    name: 'db_subnet_02'
    subnet_prefix: '10.0.5.0/24'
  }
  {
    name: 'delegated_subnet'
    subnet_prefix: '10.0.9.0/24'
  }
  {
    name: 'pvt_endpoint_subnet'
    subnet_prefix: '10.0.10.0/24'
  }
  {
    name: 'gw_subnet'
    subnet_prefix: '10.0.20.0/24'
  }
  {
    name: 'fw_subnet'
    subnet_prefix: '10.0.30.0/24'
  }
  {
    name: 'k8s_subnet'
    subnet_prefix: '10.0.128.0/19'
  }
  {
    name: 'k8s_service_cidr'
    subnet_prefix: '10.0.191.0/24'
  }
]

@description('Create a VNET with subnets')
var __vnet_name = replace(
  '${deploymentParams.enterprise_name_suffix}-${deploymentParams.loc_short_code}-${vnet_params.name_prefix}-vnet-${deploymentParams.global_uniqueness}',
  '_',
  '-'
)

resource r_vnet 'Microsoft.Network/virtualNetworks@2021-05-01' = {
  name: __vnet_name
  location: deploymentParams.location
  tags: tags
  properties: {
    addressSpace: vnet_address_prefixes
  }
}

@description('Create NSG')
var __nsg_name = replace(
  '${deploymentParams.enterprise_name_suffix}-${deploymentParams.loc_short_code}-${vnet_params.name_prefix}-nsg-${deploymentParams.global_uniqueness}',
  '_',
  '-'
)

/*
100 - AzureResourceManager
2000 - HTTP
3000 - Azure Load Balancer Health Probe
*/

resource r_vm_nsg 'Microsoft.Network/networkSecurityGroups@2021-05-01' = {
  name: __nsg_name
  location: deploymentParams.location
  tags: tags
  properties: {
    securityRules: [
      {
        name: 'AzureResourceManager'
        properties: {
          priority: 160
          protocol: 'Tcp'
          sourcePortRange: '*'
          destinationPortRange: '443'
          sourceAddressPrefix: '*'
          destinationAddressPrefix: 'AzureResourceManager'
          access: 'Allow'

          direction: 'Outbound'
        }
      }
      {
        name: 'AzureStorageAccount'
        properties: {
          priority: 170
          protocol: 'Tcp'
          sourcePortRange: '*'
          destinationPortRange: '443'
          sourceAddressPrefix: '*'
          destinationAddressPrefix: 'Storage.${deploymentParams.location}'
          access: 'Allow'
          direction: 'Outbound'
        }
      }
      {
        name: 'AzureFrontDoor'
        properties: {
          priority: 180
          protocol: 'Tcp'
          sourcePortRange: '*'
          destinationPortRange: '443'
          sourceAddressPrefix: '*'
          destinationAddressPrefix: 'AzureFrontDoor.FrontEnd'
          access: 'Allow'
          direction: 'Outbound'
        }
      }
      {
        name: 'AllowInboundSsh'
        properties: {
          priority: 250
          protocol: 'Tcp'
          access: 'Allow'
          direction: 'Inbound'
          sourceAddressPrefix: '*'
          sourcePortRange: '*'
          destinationAddressPrefix: '*'
          destinationPortRange: '22'
        }
      }
      {
        name: 'HTTP80'
        properties: {
          priority: 2001
          protocol: 'Tcp'
          access: 'Allow'
          direction: 'Inbound'
          sourceAddressPrefix: '*'
          sourcePortRange: '*'
          destinationAddressPrefix: '*'
          destinationPortRange: '80'
        }
      }
      {
        name: 'HTTP8080'
        properties: {
          priority: 2002
          protocol: 'Tcp'
          access: 'Allow'
          direction: 'Inbound'
          sourceAddressPrefix: '*'
          sourcePortRange: '*'
          destinationAddressPrefix: '*'
          destinationPortRange: '8080'
        }
      }
      {
        name: 'HTTP8089'
        properties: {
          priority: 2003
          access: 'Allow'
          direction: 'Inbound'
          protocol: 'Tcp'
          sourceAddressPrefix: 'Internet'
          sourcePortRange: '*'
          destinationAddressPrefix: '*'
          destinationPortRange: '8089'
        }
      }
      {
        name: 'app_gw_health_v1'
        properties: {
          priority: 3000
          protocol: 'TCP'
          access: 'Allow'
          direction: 'Inbound'
          sourceAddressPrefix: 'Internet'
          sourcePortRange: '65503-65534'
          destinationAddressPrefix: '*'
          destinationPortRange: '*'
        }
      }
      {
        name: 'app_gw_health_v2'
        properties: {
          priority: 3001
          protocol: 'TCP'
          access: 'Allow'
          direction: 'Inbound'
          sourceAddressPrefix: 'GatewayManager'
          sourcePortRange: '65200-65535'
          destinationAddressPrefix: '*'
          destinationPortRange: '*'
        }
      }
      {
        name: 'Outbound_Allow_All'
        properties: {
          priority: 4000
          protocol: '*'
          access: 'Allow'
          direction: 'Outbound'
          sourceAddressPrefix: '*'
          sourcePortRange: '*'
          destinationAddressPrefix: '*'
          destinationPortRange: '*'
        }
      }
    ]
  }
}
@description('Create Subnets and attach NSG')
@batchSize(1)
resource r_vnet_subnets 'Microsoft.Network/virtualNetworks/subnets@2023-04-01' = [
  for (sn, index) in subnet_cidrs: {
    name: sn.name
    parent: r_vnet
    properties: {
      addressPrefix: sn.subnet_prefix
      networkSecurityGroup: {
        id: r_vm_nsg.id
      }
    }
  }
]

// OUTPUTS
output module_metadata object = module_metadata

output vnetId string = r_vnet.id
output vnet_name string = r_vnet.name
output vnet_subnets array = r_vnet.properties.subnets

output web_subnet_01_id string = r_vnet_subnets[0].id
output db_subnet_01_id string = r_vnet_subnets[4].id
output db_subnet_02_id string = r_vnet_subnets[5].id
output pvt_endpoint_subnet_id string = r_vnet_subnets[6].id
