package eu.europeana.cloud.flink.common;

import static eu.europeana.cloud.flink.common.FollowingJobMainOperator.ERROR_STREAM_TAG;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.PATH_FLINK_JOBS_CHECKPOINTS;
import static eu.europeana.cloud.flink.simpledb.SimpleDbCassandraSourceBuilder.createCassandraSource;

import eu.europeana.cloud.flink.common.sink.CassandraClusterBuilder;
import eu.europeana.cloud.flink.common.tuples.RecordTuple;
import eu.europeana.cloud.flink.simpledb.DbEntityCreatingOperator;
import eu.europeana.cloud.flink.simpledb.DbEntityToTupleConvertingOperator;
import eu.europeana.cloud.flink.simpledb.DbErrorEntityCreatingOperator;
import eu.europeana.cloud.flink.simpledb.RecordExecutionEntity;
import eu.europeana.cloud.flink.simpledb.RecordExecutionExceptionLogEntity;
import eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys;
import java.util.Properties;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractFollowingJob<PARAMS_TYPE extends FollowingTaskParams> extends GenericJob<PARAMS_TYPE> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractFollowingJob.class);

  protected StreamExecutionEnvironment flinkEnvironment;
  private String jobName;
  private PARAMS_TYPE taskParams;

  @NotNull
  public static String createJobName(TaskParams taskParams, String jobType) {
    return jobType + " (dataset: " + taskParams.getDatasetId() + ", execution: " + taskParams.getExecutionId() + ")";
  }

  protected abstract String mainOperatorName();

  protected abstract FollowingJobMainOperator createMainOperator(Properties properties, PARAMS_TYPE taskParams);

  protected JobExecutionResult execute() throws Exception {
    JobExecutionResult result;
      LOGGER.info("Executing the Job...");
      result = flinkEnvironment.execute(jobName);
      LOGGER.info("Ended the dataset: {} execution: {}\nresult: {}", taskParams.getDatasetId(), taskParams.getExecutionId(),
          result);
    return result;
  }

  @Override
  protected void setupJob(Properties properties, PARAMS_TYPE taskParams) throws Exception {
    LOGGER.info("Creating {} for execution: {}, with execution parameters: {}",
        getClass().getSimpleName(), taskParams.getExecutionId(), taskParams);
    this.taskParams = taskParams;
    String jobType = properties.getProperty(TopologyPropertyKeys.TOPOLOGY_NAME);
    jobName = createJobName(taskParams, jobType);
    flinkEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    flinkEnvironment.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage(PATH_FLINK_JOBS_CHECKPOINTS));
    flinkEnvironment.registerJobListener(new CheckpointCleanupListener());
    DataStreamSource<RecordExecutionEntity> source = createCassandraSource(flinkEnvironment, properties, taskParams)
        //This ensure rebalancing tuples emitted by this source, so they are performed in parallel on next steps
        //TODO The command rebalance does not work for this source for some reasons. To investigate
        .setParallelism(taskParams.getParallelism());
    SingleOutputStreamOperator<RecordTuple> processStream =
        source.map(new DbEntityToTupleConvertingOperator()).name("Prepare DB entity")
              .process(createMainOperator(properties, taskParams)).name(mainOperatorName())
            .setParallelism(taskParams.getParallelism());

    SingleOutputStreamOperator<RecordExecutionEntity> resultStream =
        processStream.map(new DbEntityCreatingOperator(jobType, taskParams)).name("Create DB entity")
            .setParallelism(taskParams.getParallelism());

    SingleOutputStreamOperator<RecordExecutionExceptionLogEntity> errorStream =
        processStream.getSideOutput(ERROR_STREAM_TAG)
                     .map(new DbErrorEntityCreatingOperator(jobType, taskParams)).name("Create exception DB entity")
            .setParallelism(taskParams.getParallelism());

    CassandraClusterBuilder cassandraClusterBuilder = new CassandraClusterBuilder(properties);
    CassandraSink.addSink(resultStream)
                 .setClusterBuilder(cassandraClusterBuilder)
                 .build();
    CassandraSink.addSink(errorStream)
                 .setClusterBuilder(cassandraClusterBuilder)
                 .build();
    LOGGER.info("Created the Job");
  }
}
