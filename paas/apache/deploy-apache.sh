#!/bin/bash
set -e
echo Checking project...
oc project | grep ecloud-flink-poc
echo Project OK

set +e
oc delete secret htpasswd
oc delete configmap apache-dashboard
oc delete configmap apache-mod-security
set -e
oc create secret generic htpasswd --from-file=../../config/apache/.htpasswd
oc create configmap apache-dashboard --from-file=dashboard.conf
oc create configmap apache-mod-security --from-file=mod_security.conf

oc apply -f apache.yaml
oc apply -f apache-service.yaml
oc apply -f apache-route.yaml
echo Deploy completed - OK
