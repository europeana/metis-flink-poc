package eu.europeana.cloud.flink.clustertest;

import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterTestJob  {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterTestJob.class);
  private static StreamExecutionEnvironment flinkEnvironment;

  public ClusterTestJob(){
    flinkEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    //flinkEnvironment.setMaxParallelism(4);
    flinkEnvironment.fromSequence(1,100).map(
        new SleepOperator()
    );

  }
  public static void main(String[] args) throws Exception {
    new ClusterTestJob().execute();
  }

  private void execute() throws Exception {
    JobExecutionResult result = flinkEnvironment.execute("ClusterTestingJob");
    LOGGER.info("Endend Job. Time elapsed: {} seconds!",result.getNetRuntime(TimeUnit.SECONDS));
  }


}
