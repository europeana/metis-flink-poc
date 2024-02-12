package eu.europeana.cloud.flink.simpledb;

import com.datastax.driver.mapping.Mapper.Option;
import eu.europeana.cloud.flink.common.sink.CassandraClusterBuilder;
import eu.europeana.cloud.flink.validation.ValidationTaskParams;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.cassandra.source.CassandraSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleDbCassandraSourceBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleDbCassandraSourceBuilder.class);

  public static DataStreamSource<RecordExecutionEntity> createCassandraSource(StreamExecutionEnvironment flinkEnvironment,
      Properties properties, ValidationTaskParams taskParams) {

    CassandraSource<RecordExecutionEntity> cassandraSource = new CassandraSource<>(
        new CassandraClusterBuilder(properties),
        RecordExecutionEntity.class,
        createQuery(taskParams),
        () -> new Option[]{Option.saveNullFields(true)});

    return flinkEnvironment.fromSource(cassandraSource, WatermarkStrategy.noWatermarks(), "Cassandra records source");
  }

  @NotNull
  private static String createQuery(ValidationTaskParams taskParams) {
    String query = "select * from flink_poc.execution_record where dataset_id= '"
        + taskParams.getDatasetId()
        + "' and execution_id='"
        + taskParams.getPreviousStepId()
        + "';";
    LOGGER.info("Query: {}", query);
    return query;
  }
}
