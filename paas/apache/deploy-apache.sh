#!/bin/bash
set -e
echo Checking project...
oc project | grep ecloud-flink-poc
echo Project OK

oc apply -f apache.yaml
oc apply -f apache-service.yaml
oc apply -f apache-route.yaml

