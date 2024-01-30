package eu.europeana.cloud.flink.common;

import eu.europeana.cloud.copieddependencies.DpsRecord;
import eu.europeana.cloud.copieddependencies.DpsRecordDeserializer;
import eu.europeana.cloud.copieddependencies.TopologyPropertyKeys;
import eu.europeana.cloud.flink.common.sink.PojoSinkBuilder;
import eu.europeana.cloud.flink.common.tuples.NotificationTuple;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbstractJob {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractJob.class);
  protected static final Properties properties = new Properties();
  protected final StreamExecutionEnvironment flinkEnvironment;
  protected final String jobName;
  protected final DataStreamSource<DpsRecord> source;


  protected AbstractJob(String propertyPath) {
    try (FileInputStream fileInput = new FileInputStream(propertyPath)) {
      properties.load(fileInput);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    jobName = properties.getProperty(TopologyPropertyKeys.TOPOLOGY_NAME);
    flinkEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

    source = createSource();
    flinkEnvironment.enableCheckpointing(1000, CheckpointingMode.AT_LEAST_ONCE);
  }

  private DataStreamSource<DpsRecord> createSource() {
    final KafkaSource<DpsRecord> kafkaSource;
    KafkaRecordDeserializationSchema<DpsRecord> deserializationSchema = KafkaRecordDeserializationSchema.valueOnly(
        DpsRecordDeserializer.class);
    kafkaSource = KafkaSource.<DpsRecord>builder()
                             .setBootstrapServers(properties.getProperty(TopologyPropertyKeys.BOOTSTRAP_SERVERS))
                             .setTopics(properties.getProperty(TopologyPropertyKeys.TOPICS).split(","))
                             .setGroupId(jobName)
                             .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                             .setDeserializer(deserializationSchema)
                             .build();

    return flinkEnvironment.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Record URLs source");
  }


  protected void addSink(SingleOutputStreamOperator<NotificationTuple> resultStream) throws Exception {
        new PojoSinkBuilder(properties).build(resultStream);
        LOGGER.info("Created Cassandra Sink.");
  }


  public void execute() throws Exception {
    flinkEnvironment.execute(jobName);
  }
}
