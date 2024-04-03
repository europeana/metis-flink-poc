package eu.europeana.cloud.flink.xslt;

import static eu.europeana.cloud.flink.common.JobsParametersConstants.*;
import static eu.europeana.cloud.flink.common.utils.JobUtils.readProperties;

import eu.europeana.cloud.flink.common.AbstractFollowingJob;
import java.util.Properties;
import java.util.UUID;
import org.apache.flink.api.java.utils.ParameterTool;

public class XsltJob extends AbstractFollowingJob<XsltParams> {

  public XsltJob(Properties properties, XsltParams taskParams) throws Exception {
    super(properties, taskParams);
  }

  @Override
  protected XsltOperator createMainOperator(Properties properties, XsltParams taskParams) {
    return new XsltOperator(taskParams);
  }

  public static void main(String[] args) throws Exception {

    ParameterTool tool = ParameterTool.fromArgs(args);
    String metisDatasetId = tool.getRequired(METIS_DATASET_ID);
    XsltParams taskParams = XsltParams
        .builder()
        .datasetId(metisDatasetId)
        .previousStepId(UUID.fromString(tool.getRequired(PREVIOUS_STEP_ID)))
        .xsltUrl(tool.getRequired(XSLT_URL))
        .metisDatasetId(metisDatasetId)
        .metisDatasetName(tool.get(METIS_DATASET_NAME))
        .metisDatasetCountry(tool.get(METIS_DATASET_COUNTRY))
        .metisDatasetLanguage(tool.get(METIS_DATASET_LANGUAGE))
        .build();

    XsltJob job = new XsltJob(readProperties(tool.getRequired(CONFIGURATION_FILE_PATH)), taskParams);
    job.execute();
  }


}
