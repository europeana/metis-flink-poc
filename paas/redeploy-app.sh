#!/bin/bash
set -e
echo Redeploying flink-poc on the openshift cluster. $(oc project)
#checikng if project is valid
echo Checking project...
oc project | grep ecloud-flink-poc
echo Project OK

# Remove the deployments for the cluster
oc delete -f jobmanager-session-deployment-non-ha.yaml
oc delete -f taskmanager-session-deployment.yaml

# Remove client
oc delete -f flink-client.yaml

# Create the deployments for the cluster
oc apply -f jobmanager-session-deployment-non-ha.yaml
oc apply -f taskmanager-session-deployment.yaml

# Create client
oc apply -f flink-client.yaml
