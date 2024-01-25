package eu.europeana.cloud.flink.common;

import static eu.europeana.cloud.copieddependencies.TopologyPropertyKeys.MCS_URL;
import static eu.europeana.cloud.copieddependencies.TopologyPropertyKeys.TOPOLOGY_USER_NAME;
import static eu.europeana.cloud.copieddependencies.TopologyPropertyKeys.TOPOLOGY_USER_PASSWORD;

import eu.europeana.cloud.copieddependencies.DpsRecord;
import eu.europeana.cloud.flink.common.tuples.FileTuple;
import eu.europeana.cloud.flink.common.tuples.FileTuple.FileTupleBuilder;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import java.util.Properties;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

public class RetrieveFileOperator extends RichMapFunction<DpsRecord, FileTuple> {


  private transient FileServiceClient fileServiceClient;
  private final Properties properties;

  public RetrieveFileOperator(Properties properties) {
    this.properties=properties;
  }

  @Override
  public FileTuple map(DpsRecord dpsRecord) throws Exception {
    FileTupleBuilder builder = FileTuple.builder().dpsRecord(dpsRecord);
    if(!dpsRecord.isMarkedAsDeleted()) {
      builder.fileContent(fileServiceClient.getFile(dpsRecord.getRecordId()).readAllBytes());
    }
    return builder.build();
  }

  public void open(Configuration parameters) {
    fileServiceClient = new FileServiceClient(
        properties.getProperty(MCS_URL),
        properties.getProperty(TOPOLOGY_USER_NAME),
        properties.getProperty(TOPOLOGY_USER_PASSWORD));
  }
}
