package eu.europeana.cloud.flink.validation;

import static eu.europeana.cloud.flink.common.utils.JobUtils.readProperties;
import static eu.europeana.cloud.flink.simpledb.SimpleDbCassandraSourceBuilder.createCassandraSource;
import static eu.europeana.cloud.flink.validation.ValidationOperator.ERROR_STREAM_TAG;

import eu.europeana.cloud.flink.common.sink.CassandraClusterBuilder;
import eu.europeana.cloud.flink.common.tuples.RecordTuple;
import eu.europeana.cloud.flink.simpledb.DbEntityCreatingOperator;
import eu.europeana.cloud.flink.simpledb.DbErrorEntityCreatingOperator;
import eu.europeana.cloud.flink.simpledb.RecordExecutionExceptionLogEntity;
import eu.europeana.cloud.flink.simpledb.RecordExecutionEntity;
import eu.europeana.cloud.flink.simpledb.DbEntityToTupleConvertingOperator;
import eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys;
import java.util.Properties;
import java.util.UUID;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ValidationJob {

  private static final Logger LOGGER = LoggerFactory.getLogger(ValidationJob.class);

  protected final StreamExecutionEnvironment flinkEnvironment;

  public ValidationJob(Properties properties, ValidationTaskParams taskParams) throws Exception {
    String jobName = properties.getProperty(TopologyPropertyKeys.TOPOLOGY_NAME);
    flinkEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStreamSource<RecordExecutionEntity> source = createCassandraSource(flinkEnvironment, properties, taskParams);

    SingleOutputStreamOperator<RecordTuple> processStream =
        source.map(new DbEntityToTupleConvertingOperator())
              .process(new ValidationOperator(taskParams));

    SingleOutputStreamOperator<RecordExecutionEntity> resultStream =
        processStream.map(new DbEntityCreatingOperator(jobName, taskParams));

    SingleOutputStreamOperator<RecordExecutionExceptionLogEntity> errorStream =
        processStream.getSideOutput(ERROR_STREAM_TAG)
                     .map(new DbErrorEntityCreatingOperator(jobName, taskParams));

    CassandraClusterBuilder cassandraClusterBuilder = new CassandraClusterBuilder(properties);
    CassandraSink.addSink(resultStream)
                 .setClusterBuilder(cassandraClusterBuilder)
                 .build();
    CassandraSink.addSink(errorStream)
                 .setClusterBuilder(cassandraClusterBuilder)
                 .build();

  }


  public static void main(String[] args) throws Exception {

    ParameterTool tool = ParameterTool.fromArgs(args);
    String metisDatasetId = tool.getRequired("datasetId");
    ValidationTaskParams taskParams = ValidationTaskParams
        .builder()
        .datasetId(metisDatasetId)
        .previousStepId(UUID.fromString(tool.getRequired("previousStepId")))
        .schemaName(tool.getRequired("schemaName"))
        .rootLocation(tool.getRequired("rootLocation"))
        .schematronLocation(tool.get("schematronLocation"))
        .build();
    LOGGER.info("Creating ValidationJob for execution parameters: {}", taskParams);
    ValidationJob job = new ValidationJob(readProperties(tool.getRequired("configurationFilePath")), taskParams);
    job.execute();
  }

  private void execute() throws Exception {
    flinkEnvironment.execute();
  }

}
