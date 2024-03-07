package eu.europeana.cloud.flink.integration.ecloud.normalization;

import eu.europeana.cloud.flink.integration.ecloud.common.AbstractECloudJob;
import eu.europeana.cloud.flink.common.ReadParamsOperator;
import eu.europeana.cloud.flink.common.mcs.AddToRevisionOperator;
import eu.europeana.cloud.flink.common.mcs.RetrieveFileOperator;
import eu.europeana.cloud.flink.common.mcs.WriteRecordOperator;
import eu.europeana.cloud.flink.common.notifications.NotificationOperator;

public class ECloudNormalizationJob extends AbstractECloudJob {

  public ECloudNormalizationJob(String propertyPath) throws Exception {
    super(propertyPath);
    source.map(new ReadParamsOperator(properties)).name("Read task params")
          .map(new RetrieveFileOperator(properties)).name("Retrieve file")
          .map(new ECloudNormalizationOperator()).name("Normalize")
          .map(new WriteRecordOperator(properties)).name("Write record")
          .map(new AddToRevisionOperator(properties)).name("Add record to revision")
          .keyBy(tuple -> tuple.getTaskId())
          .map(new NotificationOperator("normalization_job", properties)).name("Save notification and progress");
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.out.println("exactly one parameter, containing configuration property file path is needed!");
    }
    ECloudNormalizationJob job = new ECloudNormalizationJob(args[0]);
    job.execute();
  }
}
