package eu.europeana.cloud.flink.clustertest;

import static eu.europeana.cloud.flink.common.JobsParametersConstants.PATH_FLINK_JOBS_CHECKPOINTS;

import eu.europeana.cloud.flink.common.CheckpointCleanupListener;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterTestJob {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterTestJob.class);

  private static StreamExecutionEnvironment flinkEnvironment;

  public static void main(String[] args) throws Exception {
    Configuration config = new Configuration();
    flinkEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    flinkEnvironment.setMaxParallelism(5);
    LOGGER.info("Checkpoint configuration: {}", PATH_FLINK_JOBS_CHECKPOINTS);
    LOGGER.info("Parallelism configuration: {} {}", flinkEnvironment.getParallelism(), flinkEnvironment.getMaxParallelism());
    flinkEnvironment.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage(PATH_FLINK_JOBS_CHECKPOINTS));
    flinkEnvironment.enableCheckpointing(2000L, CheckpointingMode.AT_LEAST_ONCE);
    flinkEnvironment.configure(config);
    flinkEnvironment.registerJobListener(new CheckpointCleanupListener());

    flinkEnvironment.fromSequence(1, 50).map(
        new SleepOperator()
    );
    JobExecutionResult result = flinkEnvironment.execute("ClusterTestingJob");
    LOGGER.info("Endend Job. Time elapsed: {} seconds!", result.getNetRuntime(TimeUnit.SECONDS));
  }

}
