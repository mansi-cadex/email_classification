# Email Classification Service Helm Chart

This Helm chart deploys the Email Classification Service to a Kubernetes cluster.

## Prerequisites

- Kubernetes 1.19+
- Helm 3.2.0+
- PV provisioner support in the underlying infrastructure

## Installing the Chart

To install the chart with the release name `email-classification`:

```bash
# Create a values file with your secrets
cat > my-values.yaml << EOF
secrets:
  dbPassword: "your-db-password"
  clientSecret: "your-client-secret"
  mongoUri: "your-mongo-uri"
  sftpPassword: "your-sftp-password"
  dbUsername: "your-db-username"
  clientId: "your-client-id"
  tenantId: "your-tenant-id"
  emailAddress: "your-email-address"
  sftpUsername: "your-sftp-username"
EOF

# Install the chart
helm install email-classification ./email-classification-chart -f my-values.yaml
```

## Configuration

The following table lists the configurable parameters of the chart and their default values.

| Parameter | Description | Default |
|-----------|-------------|---------|
| `namespace` | Kubernetes namespace | `develop` |
| `image.repository` | Container image repository | `us-central1-docker.pkg.dev/dandsltd-dev/email-classification` |
| `image.tag` | Container image tag | `latest` |
| `image.pullPolicy` | Container image pull policy | `Always` |
| `deployment.replicas` | Number of replicas | `1` |
| `service.type` | Kubernetes service type | `LoadBalancer` |
| `service.port` | Kubernetes service port | `80` |
| `service.targetPort` | Container port | `5000` |

### ConfigMap Values

All configuration values from the original ConfigMap are available under the `config` section in values.yaml.

### Secret Values

All secret values must be provided during installation. They are available under the `secrets` section in values.yaml.

## Upgrading the Chart

To upgrade the chart:

```bash
helm upgrade email-classification ./email-classification-chart -f my-values.yaml
```

## Uninstalling the Chart

To uninstall/delete the deployment:

```bash
helm uninstall email-classification
```

## Security Notes

1. Never commit the `my-values.yaml` file containing secrets to version control
2. Consider using a secrets management solution like HashiCorp Vault or AWS Secrets Manager
3. Rotate secrets regularly according to your security policies 