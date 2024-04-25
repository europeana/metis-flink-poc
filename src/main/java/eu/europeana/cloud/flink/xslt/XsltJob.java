package eu.europeana.cloud.flink.xslt;

import static eu.europeana.cloud.flink.common.JobsParametersConstants.DATASET_ID;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.EXECUTION_ID;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.METIS_DATASET_COUNTRY;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.METIS_DATASET_LANGUAGE;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.METIS_DATASET_NAME;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.PREVIOUS_STEP_ID;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.XSLT_URL;
import static eu.europeana.cloud.flink.common.utils.JobUtils.useNewIfNull;

import eu.europeana.cloud.flink.common.AbstractFollowingJob;
import eu.europeana.cloud.flink.common.JobParameters;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import org.apache.flink.api.java.utils.ParameterTool;

public class XsltJob extends AbstractFollowingJob<XsltParams> {

  public static void main(String[] args) throws Exception {
    XsltJob transformation = new XsltJob();
    transformation.executeJob(args);
  }

  protected String mainOperatorName() {
    return "Transform with XSLT";
  }

  @Override
  protected XsltOperator createMainOperator(Properties properties, XsltParams taskParams) {
    return new XsltOperator(taskParams);
  }

  @Override
  protected JobParameters<XsltParams> prepareParameters(String[] args) throws IOException {
    ParameterTool tool = ParameterTool.fromArgs(args);
    String metisDatasetId = tool.getRequired(DATASET_ID);
    XsltParams taskParams = XsltParams
        .builder()
        .datasetId(metisDatasetId)
        .executionId(useNewIfNull(tool.get(EXECUTION_ID)))
        .previousStepId(UUID.fromString(tool.getRequired(PREVIOUS_STEP_ID)))
        .xsltUrl(tool.getRequired(XSLT_URL))
        .metisDatasetId(metisDatasetId)
        .metisDatasetName(tool.get(METIS_DATASET_NAME))
        .metisDatasetCountry(tool.get(METIS_DATASET_COUNTRY))
        .metisDatasetLanguage(tool.get(METIS_DATASET_LANGUAGE))
        .build();
    return new JobParameters<>(tool, taskParams);
  }
}
