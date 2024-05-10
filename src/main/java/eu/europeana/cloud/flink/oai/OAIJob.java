package eu.europeana.cloud.flink.oai;

import static eu.europeana.cloud.flink.common.AbstractFollowingJob.createJobName;
import static eu.europeana.cloud.flink.common.FollowingJobMainOperator.ERROR_STREAM_TAG;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.DATASET_ID;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.EXECUTION_ID;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.METADATA_PREFIX;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.OAI_REPOSITORY_URL;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.PATH_FLINK_JOBS_CHECKPOINTS;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.SET_SPEC;
import static eu.europeana.cloud.flink.common.utils.JobUtils.useNewIfNull;

import eu.europeana.cloud.flink.common.CheckpointCleanupListener;
import eu.europeana.cloud.flink.common.GenericJob;
import eu.europeana.cloud.flink.common.JobParameters;
import eu.europeana.cloud.flink.common.sink.CassandraClusterBuilder;
import eu.europeana.cloud.flink.common.tuples.HarvestedRecordTuple;
import eu.europeana.cloud.flink.common.tuples.RecordTuple;
import eu.europeana.cloud.flink.oai.harvest.DeletedRecordFilter;
import eu.europeana.cloud.flink.oai.harvest.IdAssigningOperator;
import eu.europeana.cloud.flink.oai.harvest.RecordHarvestingOperator;
import eu.europeana.cloud.flink.oai.source.OAIHeadersSource;
import eu.europeana.cloud.flink.simpledb.DbEntityCreatingOperator;
import eu.europeana.cloud.flink.simpledb.DbErrorEntityCreatingOperator;
import eu.europeana.cloud.flink.simpledb.RecordExecutionEntity;
import eu.europeana.cloud.flink.simpledb.RecordExecutionExceptionLogEntity;
import eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvest;
import eu.europeana.metis.harvesting.oaipmh.OaiRecordHeader;
import java.util.Properties;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OAIJob extends GenericJob<OAITaskParams> {

  private static final Logger LOGGER = LoggerFactory.getLogger(OAIJob.class);
  protected StreamExecutionEnvironment flinkEnvironment;
  private String jobName;

  public static void main(String[] args) throws Exception {
    OAIJob oaiHarvest = new OAIJob();
    oaiHarvest.executeJob(args);
  }

  @Override
  public JobParameters<OAITaskParams> prepareParameters(String[] args) {
    ParameterTool tool = ParameterTool.fromArgs(args);
    OaiHarvest oaiHarvest = new OaiHarvest(
        tool.getRequired(OAI_REPOSITORY_URL),
        tool.getRequired(METADATA_PREFIX),
        tool.getRequired(SET_SPEC));
    String datasetId = tool.getRequired(DATASET_ID);
    OAITaskParams taskParams = OAITaskParams.builder()
                                            .oaiHarvest(oaiHarvest)
                                            .datasetId(datasetId)
                                            .metisDatasetId(datasetId)
                                            .executionId(useNewIfNull(tool.get(EXECUTION_ID)))
                                            .build();
    return new JobParameters<>(tool, taskParams);
  }

  @Override
  public JobExecutionResult execute() throws Exception {
    return flinkEnvironment.execute(jobName);
  }

  private String createSourceName(OAITaskParams taskParams) {
    return "OAI (url: " + taskParams.getOaiHarvest().getRepositoryUrl()
        + ", set: " + taskParams.getOaiHarvest().getSetSpec() +
        ", format: " + taskParams.getOaiHarvest().getMetadataPrefix() + ")";
  }

  @Override
  protected void setupJob(Properties properties, OAITaskParams taskParams) throws Exception {
    LOGGER.info("Creating {} for execution: {}, with execution parameters: {}",
        getClass().getSimpleName(), taskParams.getExecutionId(), taskParams);

    final String jobType = properties.getProperty(TopologyPropertyKeys.TOPOLOGY_NAME);
    jobName = createJobName(taskParams, jobType);
    flinkEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    flinkEnvironment.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
    CheckpointConfig checkpointConfig = flinkEnvironment.getCheckpointConfig();
    checkpointConfig.setCheckpointStorage(new FileSystemCheckpointStorage(PATH_FLINK_JOBS_CHECKPOINTS));
    checkpointConfig.enableUnalignedCheckpoints(true);
    //checkpointConfig.setForceUnalignedCheckpoints(true);
    //checkpointConfig.setMaxConcurrentCheckpoints(100);
    checkpointConfig.setCheckpointTimeout(10000L);
    checkpointConfig.setMinPauseBetweenCheckpoints(15000L);
    checkpointConfig.setTolerableCheckpointFailureNumber(100);


//    flinkEnvironment.registerJobListener(new CheckpointCleanupListener());

    CassandraClusterBuilder cassandraClusterBuilder = new CassandraClusterBuilder(properties);
    DataStreamSource<OaiRecordHeader> source = flinkEnvironment.fromSource(
        new OAIHeadersSource(taskParams, cassandraClusterBuilder), WatermarkStrategy.noWatermarks(), createSourceName(taskParams)).setParallelism(1);

    SingleOutputStreamOperator<HarvestedRecordTuple> harvestedRecordsStream =
        source.rebalance().filter(new DeletedRecordFilter()).name("Filter not deleted")
              .process(new RecordHarvestingOperator(taskParams)).name("Harvest record");

    SingleOutputStreamOperator<RecordTuple> recordsWithAssignedIdStream =
        harvestedRecordsStream.process(new IdAssigningOperator(taskParams)).name("Assign europeana id");

    SingleOutputStreamOperator<RecordExecutionEntity> resultStream =
        recordsWithAssignedIdStream.map(new DbEntityCreatingOperator(jobType, taskParams)).name("Create DB entity");



    CassandraSink.addSink(resultStream).setClusterBuilder(cassandraClusterBuilder).build();

    SingleOutputStreamOperator<RecordExecutionExceptionLogEntity> errorStream =
        harvestedRecordsStream.getSideOutput(ERROR_STREAM_TAG)
                              .union(recordsWithAssignedIdStream.getSideOutput(ERROR_STREAM_TAG))
                              .map(new DbErrorEntityCreatingOperator(jobType, taskParams)).name("Create exception DB entity");

    CassandraSink.addSink(errorStream).setClusterBuilder(cassandraClusterBuilder).build();
  }

}
