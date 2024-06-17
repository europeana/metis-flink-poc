package eu.europeana.cloud.oai.source;

import org.apache.flink.api.connector.source.SourceSplit;

public class OAISplit implements SourceSplit {

  @Override
  public String splitId() {
    return "0";
  }
}
