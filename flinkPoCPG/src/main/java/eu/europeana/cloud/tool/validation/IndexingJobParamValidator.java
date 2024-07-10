package eu.europeana.cloud.tool.validation;

import eu.europeana.cloud.flink.client.constants.postgres.JobParamName;
import org.apache.flink.api.java.utils.ParameterTool;

public class IndexingJobParamValidator implements JobParamValidator {

    @Override
    public void validate(ParameterTool parameterTool) {
        parameterTool.getRequired(JobParamName.DATASET_ID);
        parameterTool.getRequired(JobParamName.EXECUTION_ID);

        parameterTool.getRequired(JobParamName.INDEXING_PRESERVETIMESTAMPS);
        parameterTool.getRequired(JobParamName.INDEXING_PERFORMREDIRECTS);

        parameterTool.getRequired(JobParamName.INDEXING_MONGOINSTANCES);
        parameterTool.getRequired(JobParamName.INDEXING_MONGOPORTNUMBER);
        parameterTool.getRequired(JobParamName.INDEXING_MONGODBNAME);
        parameterTool.getRequired(JobParamName.INDEXING_MONGOREDIRECTDBNAME);
        parameterTool.getRequired(JobParamName.INDEXING_MONGOUSERNAME);
        parameterTool.getRequired(JobParamName.INDEXING_MONGOPASSWORD);
        parameterTool.getRequired(JobParamName.INDEXING_MONGOAUTHDB);
        parameterTool.getRequired(JobParamName.INDEXING_MONGOUSESSL);
        parameterTool.getRequired(JobParamName.INDEXING_MONGOREADPREFERENCE);
        parameterTool.getRequired(JobParamName.INDEXING_MONGOPOOLSIZE);
        parameterTool.getRequired(JobParamName.INDEXING_SOLRINSTANCES);
        parameterTool.getRequired(JobParamName.INDEXING_ZOOKEEPERINSTANCES);
        parameterTool.getRequired(JobParamName.INDEXING_ZOOKEEPERPORTNUMBER);
        parameterTool.getRequired(JobParamName.INDEXING_ZOOKEEPERCHROOT);
        parameterTool.getRequired(JobParamName.INDEXING_ZOOKEEPERDEFAULTCOLLECTION);
        parameterTool.getRequired(JobParamName.INDEXING_MONGOAPPLICATIONNAME);
    }
}
