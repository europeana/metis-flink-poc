package eu.europeana.cloud.tool;

import eu.europeana.cloud.flink.client.constants.postgres.JobParamName;
import eu.europeana.indexing.IndexingSettings;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.indexing.exception.SetupRelatedIndexingException;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;

public class IndexingSettingsGenerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexingSettingsGenerator.class);

    private final ParameterTool parameterTool;

    public IndexingSettingsGenerator(ParameterTool parameterTool) {
        this.parameterTool = parameterTool;
    }

    public IndexingSettings generate() throws IndexingException {
        IndexingSettings indexingSettings = new IndexingSettings();
        prepareSettingFor(parameterTool, indexingSettings);
        return indexingSettings;
    }

    private void prepareSettingFor(ParameterTool parameterTool, IndexingSettings indexingSettings)
            throws IndexingException {
        prepareMongoSettings(indexingSettings, parameterTool);
        prepareSolrSetting(indexingSettings, parameterTool);
        prepareZookeeperSettings(indexingSettings, parameterTool);
    }

    private void prepareMongoSettings(IndexingSettings indexingSettings, ParameterTool parameterTool)
            throws IndexingException {
        String mongoInstances = parameterTool.get(JobParamName.INDEXING_MONGOINSTANCES);
        int mongoPort = parameterTool.getInt(JobParamName.INDEXING_MONGOPORTNUMBER);
        String[] instances = mongoInstances.trim().split(",");
        for (String instance : instances) {
            indexingSettings.addMongoHost(new InetSocketAddress(instance, mongoPort));
        }
        indexingSettings
                .setMongoDatabaseName(parameterTool.get(JobParamName.INDEXING_MONGODBNAME));
        indexingSettings.setRecordRedirectDatabaseName(parameterTool.get(JobParamName.INDEXING_MONGOREDIRECTDBNAME));

        indexingSettings.setMongoCredentials(
                parameterTool.get(JobParamName.INDEXING_MONGOUSERNAME),
                parameterTool.get(JobParamName.INDEXING_MONGOPASSWORD),
                parameterTool.get(JobParamName.INDEXING_MONGOAUTHDB));

        Optional<Integer> optionalMongoPoolSize = Optional.ofNullable(parameterTool.getInt(JobParamName.INDEXING_MONGOPOOLSIZE));
        optionalMongoPoolSize.ifPresentOrElse(
                indexingSettings::setMongoMaxConnectionPoolSize,
                () -> LOGGER.warn("Mongo max connection pool size not provided"));

        if (parameterTool.get(JobParamName.INDEXING_MONGOUSESSL).equalsIgnoreCase("true")) {
            indexingSettings.setMongoEnableSsl();
        }


        indexingSettings
                .setMongoReadPreference(parameterTool.get(JobParamName.INDEXING_MONGOREADPREFERENCE));
        indexingSettings.setMongoApplicationName(parameterTool.get(JobParamName.INDEXING_MONGOAPPLICATIONNAME));

    }

    private void prepareSolrSetting(IndexingSettings indexingSettings, ParameterTool parameterTool)
            throws IndexingException {
        String solrInstances = parameterTool.get(JobParamName.INDEXING_SOLRINSTANCES);
        String[] instances = solrInstances.trim().split(",");
        try {
            for (String instance : instances) {
                indexingSettings.addSolrHost(new URI(instance));
            }
        } catch (URISyntaxException e) {
            throw new SetupRelatedIndexingException(e.getMessage(), e);
        }
    }

    private void prepareZookeeperSettings(IndexingSettings indexingSettings, ParameterTool parameterTool)
            throws IndexingException {
        String zookeeperInstances = parameterTool.get(JobParamName.INDEXING_ZOOKEEPERINSTANCES);
        int zookeeperPort = parameterTool.getInt(JobParamName.INDEXING_ZOOKEEPERPORTNUMBER);
        String[] instances = zookeeperInstances.trim().split(",");
        for (String instance : instances) {
            indexingSettings.addZookeeperHost(new InetSocketAddress(instance, zookeeperPort));
        }
        indexingSettings
                .setZookeeperChroot(parameterTool.get(JobParamName.INDEXING_ZOOKEEPERCHROOT));
        indexingSettings.setZookeeperDefaultCollection(parameterTool.get(JobParamName.INDEXING_ZOOKEEPERDEFAULTCOLLECTION));
    }
}

