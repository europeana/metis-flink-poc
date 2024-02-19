package eu.europeana.cloud.flink.oai;

import static eu.europeana.cloud.flink.common.FollowingJobMainOperator.ERROR_STREAM_TAG;
import static eu.europeana.cloud.flink.common.utils.JobUtils.readProperties;

import eu.europeana.cloud.flink.common.sink.CassandraClusterBuilder;
import eu.europeana.cloud.flink.common.tuples.HarvestedRecordTuple;
import eu.europeana.cloud.flink.common.tuples.RecordTuple;
import eu.europeana.cloud.flink.oai.harvest.DeletedOutFilter;
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

  public OAIJob(Properties properties, OAITaskParams taskParams) throws Exception {
    LOGGER.info("Creating {} for execution: {}, with execution parameters: {}",
        getClass().getSimpleName(), taskParams.getExecutionId(), taskParams);

    String jobName = properties.getProperty(TopologyPropertyKeys.TOPOLOGY_NAME);
    flinkEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStreamSource<OaiRecordHeader> source = flinkEnvironment.fromSource(
        new OAIHeadersSource(taskParams), WatermarkStrategy.noWatermarks(), "OAI Source").setParallelism(1);

    SingleOutputStreamOperator<HarvestedRecordTuple> harvestedRecordsStream =
        source.filter(new DeletedOutFilter())
              .process(new RecordHarvestingOperator(taskParams));

    SingleOutputStreamOperator<RecordTuple> recordsWithAssignedIdStream =
        harvestedRecordsStream.process(new IdAssigningOperator(taskParams));

    SingleOutputStreamOperator<RecordExecutionEntity> resultStream =
        recordsWithAssignedIdStream.map(new DbEntityCreatingOperator(jobName, taskParams));

    CassandraClusterBuilder cassandraClusterBuilder = new CassandraClusterBuilder(properties);

    CassandraSink.addSink(resultStream).setClusterBuilder(cassandraClusterBuilder).build();

    SingleOutputStreamOperator<RecordExecutionExceptionLogEntity> errorStream =
        harvestedRecordsStream.getSideOutput(ERROR_STREAM_TAG)
                              .union(recordsWithAssignedIdStream.getSideOutput(ERROR_STREAM_TAG))
                              .map(new DbErrorEntityCreatingOperator(jobName, taskParams));

    CassandraSink.addSink(errorStream).setClusterBuilder(cassandraClusterBuilder).build();
  }


  public static void main(String[] args) throws Exception {
    ParameterTool tool = ParameterTool.fromArgs(args);
    OaiHarvest oaiHarvest = new OaiHarvest(
        tool.getRequired("oaiRepositoryUrl"),
        tool.getRequired("metadataPrefix"),
        tool.getRequired("setSpec"));
    String metisDatasetId = tool.getRequired("metisDatasetId");
    OAITaskParams taskParams =
        OAITaskParams.builder()
                     .oaiHarvest(oaiHarvest)
                     .datasetId(metisDatasetId)
                     .metisDatasetId(metisDatasetId).build();
    OAIJob job = new OAIJob(readProperties(tool.getRequired("configurationFilePath")), taskParams);
    job.execute();
  }

  private void execute() throws Exception {
    flinkEnvironment.execute();
  }

}
