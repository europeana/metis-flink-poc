package eu.europeana.cloud.flink.oai.source;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.flink.api.connector.source.SourceSplit;

@Getter
@Setter
@ToString
public class OAISplit implements SourceSplit {

  private int recordDone;
  private String lastCheckpointedHeader;
  @Override
  public String splitId() {
    return "0";
  }
}
