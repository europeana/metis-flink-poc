package eu.europeana.cloud.flink.common;

import static eu.europeana.cloud.flink.common.JobsParametersConstants.PATH_FLINK_JOBS_CHECKPOINTS;

import java.net.URI;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CheckpointCleanupListener implements JobListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(CheckpointCleanupListener.class);
  @Override
  public void onJobSubmitted(@Nullable JobClient jobClient, @Nullable Throwable throwable) {
    //empty
  }

  @Override
  public void onJobExecuted(@Nullable JobExecutionResult jobExecutionResult, @Nullable Throwable throwable) {
    // Retrieve JobID
    JobID jobId = jobExecutionResult.getJobID();

    // Delete checkpoints after the job finishes
    try {
      deleteCheckpoints(jobId);
    } catch (Exception e) {
      LOGGER.error("Error deleting checkpoints: {}", e.getMessage());
    }
  }

  private void deleteCheckpoints(JobID jobId) throws Exception {
    LOGGER.info("Deleting checkpoints for JobID: {}", jobId);
    FileSystem fileSystem = FileSystem.get(new URI(PATH_FLINK_JOBS_CHECKPOINTS));
    if (fileSystem.exists(new Path(PATH_FLINK_JOBS_CHECKPOINTS + "/" + jobId))) {
      boolean deleted = fileSystem.delete(new Path(PATH_FLINK_JOBS_CHECKPOINTS + "/" + jobId), true);
      if (deleted) {
        LOGGER.info("Deleted checkpoints for JobID: {}", jobId);
      } else {
        LOGGER.info("Failed to delete checkpoints for JobID: {}", jobId);
      }
    }
  }
}
