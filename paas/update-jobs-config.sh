#!/bin/bash
set -e
echo Deploing Flink on the openshift cluster. $(oc project)
#checikng if project is valid
echo Checking project...
oc project | grep ecloud-flink-poc
echo Project OK

oc delete secret jobs-config
oc create secret generic jobs-config --from-file=config/jobs