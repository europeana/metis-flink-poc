Info about running the POC on the openshift cluster:

1. Prepare config directory:
1.1. Rename config.template directory to new name: config
1.2. For every config file in the directory fill the remaining required data

2. docker/build-docker-images.sh - building all the docker images:
2.1. flink_java21 - original flink 1.18 with java updated
2.2. flink-node - flink image with additional diagnostic tools
2.3. flink-node-with-application - flin-node image with application contained in it.
To only update application code only the last image need to be built.

3. Deploying on the openshift server:
3.1. paas/update-jobs-config.sh - copy config files from the config directory to the openshift secrets
3.2. paas/apache/deploy-apache.sh -deploys apache server used for web dashboard on a openshift cluster it needs to have file: config/apache/.htpasswd generated  
3.3. paas/deploy.sh - deploy the whole cluster servers/services except the apache server on a openshift cluster, could be also use for config update

4Updating the cluster
4.1. redeploy-app.sh - push the images again with a script in the openshift without doing the volumes part. only the containers.

