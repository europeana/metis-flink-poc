package eu.europeana.cloud.flink.simpledb;

import static java.util.Objects.requireNonNull;

import com.datastax.driver.mapping.Mapper.Option;
import eu.europeana.cloud.flink.common.FollowingTaskParams;
import eu.europeana.cloud.flink.common.sink.CassandraClusterBuilder;
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
      Properties properties, FollowingTaskParams taskParams) {

    CassandraSource<RecordExecutionEntity> cassandraSource = new CassandraSource<>(
        new CassandraClusterBuilder(properties),
        RecordExecutionEntity.class,
        createQuery(taskParams),
        () -> new Option[]{Option.saveNullFields(true)});

    return flinkEnvironment.fromSource(cassandraSource, WatermarkStrategy.noWatermarks(), "Cassandra records source");
  }

  @NotNull
  private static String createQuery(FollowingTaskParams taskParams) {
    String query = String.format(
        "select * from flink_poc.execution_record where dataset_id= '%s' and execution_id='%s';",
        requireNonNull(taskParams.getDatasetId()),
        requireNonNull(taskParams.getPreviousStepId()));

    LOGGER.info("Query: {}", query);
    return query;
  }
}
