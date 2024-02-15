package eu.europeana.cloud.flink.xslt;

import static eu.europeana.cloud.flink.common.utils.JobUtils.readProperties;

import eu.europeana.cloud.flink.common.AbstractFollowingJob;
import java.util.Properties;
import java.util.UUID;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XsltJob extends AbstractFollowingJob<XsltParams> {

  private static final Logger LOGGER = LoggerFactory.getLogger(XsltJob.class);

  public XsltJob(Properties properties, XsltParams taskParams) throws Exception {
    super(properties, taskParams);
  }

  @Override
  protected XsltOperator createMainOperator(XsltParams taskParams) {
    return new XsltOperator(taskParams);
  }

  public static void main(String[] args) throws Exception {

    ParameterTool tool = ParameterTool.fromArgs(args);
    String metisDatasetId = tool.getRequired("metisDatasetId");
    XsltParams taskParams = XsltParams
        .builder()
        .datasetId(metisDatasetId)
        .previousStepId(UUID.fromString(tool.getRequired("previousStepId")))
        .xsltUrl(tool.getRequired("xsltUrl"))
        .metisDatasetId(metisDatasetId)
        .metisDatasetName(tool.get("metisDatasetName"))
        .metisDatasetCountry(tool.get("metisDatasetCountry"))
        .metisDatasetLanguage(tool.get("metisDatasetLanguage"))
        .build();

    XsltJob job = new XsltJob(readProperties(tool.getRequired("configurationFilePath")), taskParams);
    job.execute();
  }


}
