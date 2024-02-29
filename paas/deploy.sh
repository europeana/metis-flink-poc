#!/bin/bash
set -e
echo Deploing Flink on the openshift cluster. $(oc project)
#checikng if project is valid
echo Checking project...
oc project | grep ecloud-flink-poc
echo Project OK

# Configuration and service definition
oc apply -f flink-configuration-configmap.yaml
oc apply -f jobmanager-service.yaml

# Create the deployments for the cluster
oc apply -f jobmanager-session-deployment-non-ha.yaml
oc apply -f taskmanager-session-deployment.yaml