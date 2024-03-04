#!/bin/bash
set -e
echo Checking project...
oc project | grep ecloud-flink-poc
echo Project OK

oc create secret generic htpasswd --from-file=config/apache/.htpasswd
oc create configmap apache-dashboard --from-file=dashboard.conf