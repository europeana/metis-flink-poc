package eu.europeana.cloud.flink.clustertest;

import static eu.europeana.cloud.flink.common.JobsParametersConstants.PATH_FLINK_JOBS_CHECKPOINTS;

import eu.europeana.cloud.flink.common.CheckpointCleanupListener;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterTestJob {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterTestJob.class);

  private static StreamExecutionEnvironment flinkEnvironment;

  public ClusterTestJob() {
    flinkEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    //flinkEnvironment.setMaxParallelism(4);

    flinkEnvironment.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage(PATH_FLINK_JOBS_CHECKPOINTS));
    flinkEnvironment.enableCheckpointing(1000L, CheckpointingMode.AT_LEAST_ONCE);
    flinkEnvironment.registerJobListener(new CheckpointCleanupListener());
    flinkEnvironment.fromSequence(1, 200).map(
        new SleepOperator()
    );
  }

  public static void main(String[] args) throws Exception {
    new ClusterTestJob().execute();
  }

  private void execute() throws Exception {
    JobExecutionResult result = flinkEnvironment.execute("ClusterTestingJob");
    LOGGER.info("Endend Job. Time elapsed: {} seconds!", result.getNetRuntime(TimeUnit.SECONDS));
  }



}
