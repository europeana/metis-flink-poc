package eu.europeana.cloud.flink.oai;

import static eu.europeana.cloud.flink.common.utils.JobUtils.readProperties;

import eu.europeana.cloud.flink.common.sink.CassandraClusterBuilder;
import eu.europeana.cloud.flink.oai.harvest.DeletedOutFilter;
import eu.europeana.cloud.flink.oai.harvest.RecordHarvestingOperator;
import eu.europeana.cloud.flink.simpledb.DbEntityCreatingOperator;
import eu.europeana.cloud.flink.simpledb.RecordExecutionEntity;
import eu.europeana.cloud.flink.oai.harvest.IdAssigningOperator;
import eu.europeana.cloud.flink.oai.source.OAIHeadersSource;
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
    String jobName = properties.getProperty(TopologyPropertyKeys.TOPOLOGY_NAME);
    flinkEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStreamSource<OaiRecordHeader> source = flinkEnvironment.fromSource(
        new OAIHeadersSource(taskParams), WatermarkStrategy.noWatermarks(), "OAI Source").setParallelism(1);

    SingleOutputStreamOperator<RecordExecutionEntity> resultStream =
        source.filter(new DeletedOutFilter())
              .map(new RecordHarvestingOperator(taskParams))
              .map(new IdAssigningOperator(taskParams))
              .map(new DbEntityCreatingOperator(jobName, taskParams));

    CassandraSink.addSink(resultStream)
                 .setClusterBuilder(new CassandraClusterBuilder(properties))
                 .build();
  }


  public static void main(String[] args) throws Exception {
    //    OaiHarvest oaiHarvest = new OaiHarvest("https://metis-repository-rest.test.eanadev.org/repository/oai", "edm",
    //        "ecloud_e2e_tests");
    //
    //    OAIHarvestingTaskInformation taskParams =
    //        OAIHarvestingTaskInformation.builder()
    //                                    .oaiHarvest(oaiHarvest)
    //                                    .metisDatasetId("1")
    //                                    .executionId(UUIDs.timeBased())
    //                                    .build();

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
