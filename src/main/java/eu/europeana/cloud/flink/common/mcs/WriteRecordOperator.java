package eu.europeana.cloud.flink.common.mcs;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.MCS_URL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.TOPOLOGY_USER_NAME;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.TOPOLOGY_USER_PASSWORD;

import eu.europeana.cloud.flink.common.tuples.FileTuple;
import eu.europeana.cloud.flink.common.tuples.RecordParams;
import eu.europeana.cloud.flink.common.tuples.TaskParams;
import eu.europeana.cloud.flink.common.tuples.WrittenRecordTuple;
import eu.europeana.cloud.flink.common.tuples.WrittenRecordTuple.WrittenRecordTupleBuilder;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.dps.storm.utils.UUIDWrapper;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.util.Properties;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteRecordOperator extends RichMapFunction<FileTuple, WrittenRecordTuple> {

  private static final Logger LOGGER = LoggerFactory.getLogger(WriteRecordOperator.class);
  private final Properties properties;
  private transient RecordServiceClient recordServiceClient;

  public WriteRecordOperator(Properties properties) {
    this.properties = properties;
  }

  @Override
  public WrittenRecordTuple map(FileTuple tuple) throws Exception {
    TaskParams taskParams = tuple.getTaskParams();
    RecordParams recordParams = tuple.getRecordParams();
    WrittenRecordTupleBuilder result = WrittenRecordTuple.builder()
                                                         .taskParams(taskParams)
                                                         .recordParams(recordParams);
    if (!tuple.isMarkedAsDeleted()) {
      URI newUri = createRepresentationAndUploadFile(taskParams, recordParams, tuple.getFileContent());
      result.newResourceUrl(newUri.toString());
    }
    return result.build();
  }

  protected java.net.URI createRepresentationAndUploadFile(TaskParams taskParams, RecordParams recordParams, byte[] fileContent)
      throws Exception {
    String recordId = recordParams.getRecordId();

    URI savedFileUrl = recordServiceClient.createRepresentation(
        recordParams.getCloudId(),
        taskParams.getNewRepresentationName(),
        taskParams.getProviderId(),
        UUIDWrapper.generateRepresentationVersion(taskParams.getSentTimestamp(), recordParams.getRecordId()),
        taskParams.getDataSetId(),
        new ByteArrayInputStream(fileContent),
        UUIDWrapper.generateRepresentationFileName(recordId),
        taskParams.getOutputMimeType()
    );

    LOGGER.info("Saved file in MCS url: {}", savedFileUrl);
    LOGGER.debug("Saved file content: {}", fileContent);
    return savedFileUrl;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    recordServiceClient = new RecordServiceClient(
        properties.getProperty(MCS_URL),
        properties.getProperty(TOPOLOGY_USER_NAME),
        properties.getProperty(TOPOLOGY_USER_PASSWORD));
    LOGGER.info("Created operator writing files from MCS.");
  }
}
