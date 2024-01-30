package eu.europeana.cloud.flink.normalization;

import eu.europeana.cloud.flink.common.AbstractJob;
import eu.europeana.cloud.flink.common.ReadParamsOperator;
import eu.europeana.cloud.flink.common.mcs.AddToRevisionOperator;
import eu.europeana.cloud.flink.common.mcs.RetrieveFileOperator;
import eu.europeana.cloud.flink.common.mcs.WriteRecordOperator;
import eu.europeana.cloud.flink.common.notifications.NotificationOperator;

public class NormalizationJob extends AbstractJob {

  public NormalizationJob(String propertyPath) throws Exception {
    super(propertyPath);
    source.map(new ReadParamsOperator(properties))
          .map(new RetrieveFileOperator(properties))
          .map(new NormalizationOperator())
          .map(new WriteRecordOperator(properties))
          .map(new AddToRevisionOperator(properties))
          .keyBy(tuple -> tuple.getTaskId())
          .map(new NotificationOperator("normalization_job", properties));
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.out.println("exactly one parameter, containing configuration property file path is needed!");
    }
    NormalizationJob job = new NormalizationJob(args[0]);
    job.execute();
  }
}
