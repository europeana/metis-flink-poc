package eu.europeana.cloud.flink.integration.ecloud.normalization;

import eu.europeana.cloud.flink.common.AbstractJob;
import eu.europeana.cloud.flink.common.ReadParamsOperator;
import eu.europeana.cloud.flink.common.mcs.AddToRevisionOperator;
import eu.europeana.cloud.flink.common.mcs.RetrieveFileOperator;
import eu.europeana.cloud.flink.common.mcs.WriteRecordOperator;
import eu.europeana.cloud.flink.common.notifications.NotificationOperator;

public class ECloudNormalizationJob extends AbstractJob {

  public ECloudNormalizationJob(String propertyPath) throws Exception {
    super(propertyPath);
    source.map(new ReadParamsOperator(properties))
          .map(new RetrieveFileOperator(properties))
          .map(new ECloudNormalizationOperator())
          .map(new WriteRecordOperator(properties))
          .map(new AddToRevisionOperator(properties))
          .keyBy(tuple -> tuple.getTaskId())
          .map(new NotificationOperator("normalization_job", properties));
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.out.println("exactly one parameter, containing configuration property file path is needed!");
    }
    ECloudNormalizationJob job = new ECloudNormalizationJob(args[0]);
    job.execute();
  }
}
