package eu.europeana.cloud.flink.oai;

import static eu.europeana.cloud.flink.common.AbstractFollowingJob.createJobName;
import static eu.europeana.cloud.flink.common.FollowingJobMainOperator.ERROR_STREAM_TAG;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.*;
import static eu.europeana.cloud.flink.common.utils.JobUtils.readProperties;

import eu.europeana.cloud.flink.common.sink.CassandraClusterBuilder;
import eu.europeana.cloud.flink.common.tuples.HarvestedRecordTuple;
import eu.europeana.cloud.flink.common.tuples.RecordTuple;
import eu.europeana.cloud.flink.oai.harvest.DeletedRecordFilter;
import eu.europeana.cloud.flink.oai.harvest.RecordHarvestingOperator;
import eu.europeana.cloud.flink.simpledb.DbEntityCreatingOperator;
import eu.europeana.cloud.flink.simpledb.DbErrorEntityCreatingOperator;
import eu.europeana.cloud.flink.simpledb.RecordExecutionEntity;
import eu.europeana.cloud.flink.oai.harvest.IdAssigningOperator;
import eu.europeana.cloud.flink.oai.source.OAIHeadersSource;
import eu.europeana.cloud.flink.simpledb.RecordExecutionExceptionLogEntity;
import eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvest;
import eu.europeana.metis.harvesting.oaipmh.OaiRecordHeader;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OAIJob {

  private static final Logger LOGGER = LoggerFactory.getLogger(OAIJob.class);
  protected final StreamExecutionEnvironment flinkEnvironment;
  private final String jobName;

  public OAIJob(Properties properties, OAITaskParams taskParams) throws Exception {
    LOGGER.info("Creating {} for execution: {}, with execution parameters: {}",
        getClass().getSimpleName(), taskParams.getExecutionId(), taskParams);

    final String jobType = properties.getProperty(TopologyPropertyKeys.TOPOLOGY_NAME);
    jobName = createJobName(taskParams, jobType);
    flinkEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStreamSource<OaiRecordHeader> source = flinkEnvironment.fromSource(
        new OAIHeadersSource(taskParams), WatermarkStrategy.noWatermarks(), createSourceName(taskParams)).setParallelism(1);

    SingleOutputStreamOperator<HarvestedRecordTuple> harvestedRecordsStream =
        source.filter(new DeletedRecordFilter()).name("Filter not deleted")
              .process(new RecordHarvestingOperator(taskParams)).name("Harvest record");

    SingleOutputStreamOperator<RecordTuple> recordsWithAssignedIdStream =
        harvestedRecordsStream.process(new IdAssigningOperator(taskParams)).name("Assign europeana id");

    SingleOutputStreamOperator<RecordExecutionEntity> resultStream =
        recordsWithAssignedIdStream.map(new DbEntityCreatingOperator(jobType, taskParams)).name("Create DB entity");

    CassandraClusterBuilder cassandraClusterBuilder = new CassandraClusterBuilder(properties);

    CassandraSink.addSink(resultStream).setClusterBuilder(cassandraClusterBuilder).build();

    SingleOutputStreamOperator<RecordExecutionExceptionLogEntity> errorStream =
        harvestedRecordsStream.getSideOutput(ERROR_STREAM_TAG)
                              .union(recordsWithAssignedIdStream.getSideOutput(ERROR_STREAM_TAG))
                              .map(new DbErrorEntityCreatingOperator(jobType, taskParams)).name("Create exception DB entity");

    CassandraSink.addSink(errorStream).setClusterBuilder(cassandraClusterBuilder).build();
  }

  private String createSourceName(OAITaskParams taskParams) {
    return "OAI (url: "+taskParams.getOaiHarvest().getRepositoryUrl()
        +", set: "+taskParams.getOaiHarvest().getSetSpec()+
        ", format: "+taskParams.getOaiHarvest().getMetadataPrefix() +")";
  }

  public static void main(String[] args) throws Exception {
    ParameterTool tool = ParameterTool.fromArgs(args);
    OaiHarvest oaiHarvest = new OaiHarvest(
        tool.getRequired(OAI_REPOSITORY_URL),
        tool.getRequired(METADATA_PREFIX),
        tool.getRequired(SET_SPEC));
    String metisDatasetId = tool.getRequired(METIS_DATASET_ID);
    OAITaskParams taskParams =
        OAITaskParams.builder()
                     .oaiHarvest(oaiHarvest)
                     .datasetId(metisDatasetId)
                     .metisDatasetId(metisDatasetId).build();
    OAIJob job = new OAIJob(readProperties(tool.getRequired(CONFIGURATION_FILE_PATH)), taskParams);
    job.execute();
  }

  private void execute() throws Exception {
    flinkEnvironment.execute(jobName);
  }

}
