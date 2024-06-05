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


## Minikube deployment 

### Prepare Cassandra
- Prepare docker container Cassandra:  
   `docker network create cassandra`  
   `docker run -d --name cassandra --hostname cassandra --network cassandra -p 9042:9042 cassandra:3.11.17`  
  </br>
- Create keyspace in Cassandra:  
    `create keyspace flink_poc with replication = {'class': 'SimpleStrategy', 'replication_factor': 1};`  
  </br>
- Create the tables inside the newly created keyspace using the file `src/resources/simple-db-cassandra-schema.cql`  

### Prepare Private docker registry access
- Login to the PSNC registry:  
    `docker login --username <username> --password <passowrd> <registry-url>/ecloud-poc`  
  </br>
- Create the secret containing the docker registry credentials:  
    `kubectl create secret generic psnc-registry-credentials --from-file=.dockerconfigjson=<user-path>/.docker/config.json --type=kubernetes.io/dockerconfigjson`
  </br>
- Deployments that use images from the PSNC registry should contain the property:  
    ```
    imagePullSecrets:  
        - name: psnc-registry-credentials
    ```  

### Prepare configuration properties for the jobs
- Copy the `config.template` to `config` directory and update all the property files under the `config/jobs` directory.  
- Create the secret pointing to this newly created directory:
    `kubectl create secret generic jobs-config --from-file=config/jobs` 

### Prepare kubernetes flink deployments
- Deploy kubernetes objects:  
    The `jobmanager-ingress.yaml` contains a `nip.io` link. Update the ip prefix to the minikube ip obtained from:  
    `minikube ip`  

    ```
    kubectl create -f flink-configuration-configmap.yaml
    kubectl create -f jobmanager-service.yaml  
    kubectl create -f jobmanager-ingress.yaml  
    kubectl create -f jobmanager-checkpoints-volume.yaml  
    kubectl create -f jobmanager-checkpoints-volume-claim.yaml  
    kubectl create -f jobmanager-session-deployment-non-ha.yaml  
    kubectl create -f taskmanager-session-deployment.yaml
    ```  

### Communication with the flink cluster
- Accessing the ui by using the url link used in the `jobmanager-ingress.yaml` e.g.
    `https://flink-jobmanager-192.168.49.2.nip.io`  
  </br>
- Deploy the empty.jar:
    `curl -k -X POST -H "Expect:" -F "jarfile=@<path-to-jar>/empty-1.jar" "https://flink-jobmanager-192.168.49.2.nip.io/jars/upload"`  
  </br>
- Verify jar is present and get its id:
    `curl -k -X GET "https://flink-jobmanager-192.168.49.2.nip.io/jars"`  
  </br>
- Run OAI job:
    ```
    curl -k -X POST 'https://flink-jobmanager-192.168.49.2.nip.io/jars/c021b15a-d8dc-47ba-9fee-1c837ca62c59_empty-1.jar/run?entry-class=eu.europeana.cloud.flink.oai.OAIJob' \
    -H 'Content-Type: application/json' \
    --data-raw '{
    "entryClass": "eu.europeana.cloud.flink.oai.OAIJob",
    "programArgs": "--parallelism 2 --configurationFilePath /jobs-config/oai_job.properties --oaiRepositoryUrl https://metis-repository-rest.test.eanadev.org/repository/oai --metadataPrefix edm --setSpec spring_poc_dataset_with_validation_error --datasetId 1",
    "savepointPath": null,
    "allowNonRestoredState": false
    }'
    ```

- Run Validation-External job:
    ```
    curl -k -X POST 'https://flink-jobmanager-192.168.49.2.nip.io/jars/937e4410-e269-4020-8e5d-38ec717a4a68_empty-1.jar/run?entry-class=eu.europeana.cloud.flink.validation.ValidationJob' \
    -H 'Content-Type: application/json' \
    --data-raw '{
    "entryClass": "eu.europeana.cloud.flink.validation.ValidationJob",
    "programArgs": "--parallelism 2 --configurationFilePath /jobs-config/validation_job.properties --datasetId 1 --previousStepId 046769e0-2340-11ef-9032-1dc7987fb168 --schemaName http://ftp.eanadev.org/schema_zips/europeana_schemas-20220809.zip --rootLocation EDM.xsd --schematronLocation schematron/schematron.xsl",
    "savepointPath": null,
    "allowNonRestoredState": false
    }'
    ```

- Run Transformation job:
    ```
    curl -k -X POST 'https://flink-jobmanager-192.168.49.2.nip.io/jars/0e4b7962-59c1-4b8f-b25b-f5a23fa28fec_empty-1.jar/run?entry-class=eu.europeana.cloud.flink.xslt.XsltJob' \
    -H 'Content-Type: application/json' \
    --data-raw '{
    "entryClass": "eu.europeana.cloud.flink.xslt.XsltJob",
    "programArgs": "--parallelism 2 --configurationFilePath /jobs-config/xslt_job.properties --datasetId 1 --previousStepId d1e23680-1da4-11ef-a5a1-ebe13220a167 --xsltUrl https://metis-core-rest.test.eanadev.org/datasets/xslt/default",
    "savepointPath": null,
    "allowNonRestoredState": false
    }'
    ```

- Get jobs overview:  
    `curl -k -X GET "https://flink-jobmanager-192.168.49.2.nip.io/jobs/overview"`  
  </br>
- Cancel a job:
    `curl -k -X PATCH "https://flink-jobmanager-192.168.49.2.nip.io/jobs/<jobid>"`