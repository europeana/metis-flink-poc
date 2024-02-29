# Configuration and service definition
oc apply -f flink-configuration-configmap.yaml
oc apply -f jobmanager-service.yaml

# Create the deployments for the cluster
oc apply -f jobmanager-session-deployment-non-ha.yaml
oc apply -f taskmanager-session-deployment.yaml