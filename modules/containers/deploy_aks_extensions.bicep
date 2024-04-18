// SET MODULE DATE
param module_metadata object = {
  module_last_updated: '2023-06-25'
  owner: 'miztiik@github'
}

param cluster_name string = 'm-cluster'

@description('Get AKS Cluster Reference')
resource r_aks_c_ref 'Microsoft.ContainerService/managedClusters@2021-03-01' existing = {
  name: cluster_name
}

resource fluxExtensions 'Microsoft.KubernetesConfiguration/extensions@2022-03-01' = {
  scope: r_aks_c_ref
  name: 'flux'
  properties: {
    extensionType: 'microsoft.flux'
  }

}
resource fluxConfig 'Microsoft.KubernetesConfiguration/fluxConfigurations@2022-03-01' = {
  name: 'bicep-fluxconfig'
  scope: r_aks_c_ref
  properties: {
    scope: 'cluster'
    namespace: 'cluster-config'
    sourceKind: 'GitRepository'
    gitRepository: {
      url: 'https://github.com/Azure/gitops-flux2-kustomize-helm-mt'
      repositoryRef: {
        branch: 'main'
      }
      syncIntervalInSeconds: 120
    }
    kustomizations: {
      'infra': {
        path: './infrastructure'
        syncIntervalInSeconds: 120
      }
      'apps': {
        path: './apps/production'
        syncIntervalInSeconds: 120
        dependsOn: [
          'infra'
        ]
      }
    }
    // configurationProtectedSettings: {
    //   'sshPrivateKey': '<base64-encoded-pem-private-key>'
    // }
  }
  dependsOn: [
    fluxExtensions
  ]

}

// OUTPUTS
output module_metadata object = module_metadata
