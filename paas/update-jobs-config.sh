#!/bin/bash
set -e
echo Deploying Flink on the openshift cluster. $(oc project)
#checikng if project is valid
echo Checking project...
oc project | grep ecloud-flink-poc
echo Project OK

set +e
oc delete secret jobs-config
set -e
oc create secret generic jobs-config --from-file=../config/jobs
