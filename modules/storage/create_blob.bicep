// SET MODULE DATE
param module_metadata object = {
  module_last_updated: '2023-12-16'
  owner: 'miztiik@github'
}

param deploymentParams object
param sa_params object
param sa_name string

param misc_sa_name string

// Get reference of SA
resource r_sa 'Microsoft.Storage/storageAccounts@2021-06-01' existing = {
  name: sa_name
}

// Create a blob storage container in the storage account
resource r_blob_svc 'Microsoft.Storage/storageAccounts/blobServices@2021-06-01' = {
  parent: r_sa
  name: 'default'
  properties: {
    cors: {
      corsRules: []
    }
  }
}

resource r_blobContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2021-04-01' = {
  parent: r_blob_svc
  name: '${sa_params.blob_name_prefix}-blob-${deploymentParams.global_uniqueness}'
  properties: {
    publicAccess: 'None'
  }
}

// OUTPUTS
output module_metadata object = module_metadata

output blob_container_id string = r_blobContainer.id
output blob_container_name string = r_blobContainer.name

// Get reference of SA
resource r_misc_sa_ref 'Microsoft.Storage/storageAccounts@2021-06-01' existing = {
  name: misc_sa_name
}

// Create a blob storage container in the storage account
resource r_misc_sa_default_container 'Microsoft.Storage/storageAccounts/blobServices@2021-06-01' = {
  parent: r_misc_sa_ref
  name: 'default'
  properties: {
    cors: {
      corsRules: []
    }
  }
}

// resource r_blobContainer_1 'Microsoft.Storage/storageAccounts/blobServices/containers@2021-04-01' = {
//   parent: r_blobSvc_1
//   name: '${storageAccountParams.blobNamePrefix}-blob-${deploymentParams.global_uniqueness}'
//   properties: {
//     publicAccess: 'None'
//   }
// }
// output blobContainerId_1 string = r_blobContainer_1.id
// output blobContainerName_1 string = r_blobContainer_1.name
