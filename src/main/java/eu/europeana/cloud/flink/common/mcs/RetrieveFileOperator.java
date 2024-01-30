package eu.europeana.cloud.flink.common.mcs;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.MCS_URL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.TOPOLOGY_USER_NAME;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.TOPOLOGY_USER_PASSWORD;

import eu.europeana.cloud.flink.common.tuples.FileTuple;
import eu.europeana.cloud.flink.common.tuples.FileTuple.FileTupleBuilder;
import eu.europeana.cloud.flink.common.tuples.TaskRecordTuple;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import java.util.Properties;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetrieveFileOperator extends RichMapFunction<TaskRecordTuple, FileTuple> {
  private static final Logger LOGGER = LoggerFactory.getLogger(RetrieveFileOperator.class);
  private transient FileServiceClient fileServiceClient;
  private final Properties properties;

  public RetrieveFileOperator(Properties properties) {
    this.properties = properties;
  }

  @Override
  public FileTuple map(TaskRecordTuple tuple) throws Exception {
    FileTupleBuilder builder = FileTuple.builder()
                                        .taskParams(tuple.getTaskParams())
                                        .recordParams(tuple.getRecordParams());
    if (!tuple.isMarkedAsDeleted()) {
      byte[] fileContent = fileServiceClient.getFile(tuple.getFileUrl()).readAllBytes();
      builder.fileContent(fileContent);
      LOGGER.info("Loaded file from MCS  url: {}", tuple.getFileUrl());
      LOGGER.debug("File content: {}", fileContent);
    }

    return builder.build();
  }

  public void open(Configuration parameters) {
    fileServiceClient = new FileServiceClient(
        properties.getProperty(MCS_URL),
        properties.getProperty(TOPOLOGY_USER_NAME),
        properties.getProperty(TOPOLOGY_USER_PASSWORD));
    LOGGER.info("Created operator reading files from MCS.");
  }
}
