#!/bin/bash
set -e
echo Deploing Flink on the openshift cluster. $(oc project)
#checikng if project is valid
echo Checking project...
oc project | grep ecloud-flink-poc
echo Project OK

#Configure volumes
oc apply -f deployments-volume.yaml
oc apply -f web-upload-volume.yaml
oc apply -f job-manager-working-volume.yaml
oc apply -f job-manager-dumps-volume.yaml
# Configuration and service definition
oc apply -f flink-configuration-configmap.yaml
oc apply -f jobmanager-service.yaml

# Create the deployments for the cluster
oc apply -f jobmanager-session-deployment-non-ha.yaml
oc apply -f taskmanager-session-deployment.yaml

# Create client
oc apply -f flink-client.yaml