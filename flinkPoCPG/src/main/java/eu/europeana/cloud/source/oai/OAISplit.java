package eu.europeana.cloud.source.oai;

import org.apache.flink.api.connector.source.SourceSplit;

public class OAISplit implements SourceSplit {

  @Override
  public String splitId() {
    return "0";
  }
}
