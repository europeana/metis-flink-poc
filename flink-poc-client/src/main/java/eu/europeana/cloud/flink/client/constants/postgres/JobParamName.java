package eu.europeana.cloud.flink.client.constants.postgres;

public class JobParamName {

    public static final String OPERATOR_PARALLELISM = "OPERATOR_PARALLELISM";
    public static final String TASK_ID = "taskId";
    public static final String VALIDATION_TYPE = "validationType";
    public static final String DATASET_ID = "datasetId";
    public static final String EXECUTION_ID = "executionId";
    public static final String METIS_DATASET_NAME = "metisDatasetName";
    public static final String METIS_DATASET_COUNTRY = "metisDatasetCountry";
    public static final String METIS_DATASET_LANGUAGE = "metisDatasetLanguage";
    public static final String METIS_XSLT_URL = "metisXsltUrl";
    public static final String CHUNK_SIZE = "chunkSize";
    public static final String DEREFERENCE_SERVICE_URL = "DEREFERENCE_SERVICE_URL";
    public static final String ENRICHMENT_ENTITY_MANAGEMENT_URL = "ENRICHMENT_ENTITY_MANAGEMENT_URL";
    public static final String ENRICHMENT_ENTITY_API_URL = "ENRICHMENT_ENTITY_API_URL";
    public static final String ENRICHMENT_ENTITY_API_KEY = "ENRICHMENT_ENTITY_API_KEY";

    public static final String DATASOURCE_URL = "datasource.url";
    public static final String DATASOURCE_USERNAME = "datasource.username";
    public static final String DATASOURCE_PASSWORD = "datasource.password";

    public static final String INDEXING_PRESERVETIMESTAMPS = "preserveTimestamps";
    public static final String INDEXING_PERFORMREDIRECTS = "performRedirects";

    public static final String INDEXING_MONGOINSTANCES = "indexing.mongoInstances";
    public static final String INDEXING_MONGOPORTNUMBER = "indexing.mongoPortNumber";
    public static final String INDEXING_MONGODBNAME = "indexing.mongoDbName";
    public static final String INDEXING_MONGOREDIRECTDBNAME = "indexing.mongoRedirectsDbName";
    public static final String INDEXING_MONGOUSERNAME = "indexing.mongoUsername";
    public static final String INDEXING_MONGOPASSWORD = "indexing.mongoPassword";
    public static final String INDEXING_MONGOAUTHDB = "indexing.mongoAuthDB";
    public static final String INDEXING_MONGOUSESSL = "indexing.mongoUseSSL";
    public static final String INDEXING_MONGOREADPREFERENCE = "indexing.mongoReadPreference";
    public static final String INDEXING_MONGOPOOLSIZE = "indexing.mongoPoolSize";
    public static final String INDEXING_SOLRINSTANCES = "indexing.solrInstances";
    public static final String INDEXING_ZOOKEEPERINSTANCES = "indexing.zookeeperInstances";
    public static final String INDEXING_ZOOKEEPERPORTNUMBER = "indexing.zookeeperPortNumber";
    public static final String INDEXING_ZOOKEEPERCHROOT = "indexing.zookeeperChroot";
    public static final String INDEXING_ZOOKEEPERDEFAULTCOLLECTION = "indexing.zookeeperDefaultCollection";
    public static final String INDEXING_MONGOAPPLICATIONNAME = "indexing.mongoApplicationName";

    //OAI
    public static final String SET_SPEC = "setSpec";
    public static final String METADATA_PREFIX = "metadataPrefix";
    public static final String OAI_REPOSITORY_URL = "oaiRepositoryUrl";

    private JobParamName() {
    }

}
