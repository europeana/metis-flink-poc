package eu.europeana.cloud.flink.common.tuples;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class RecordParams {

  private String resourceUrl;
  private String cloudId;
  private String representationName;
  private String representationVersion;
  private boolean markedAsDeleted;

  public String getRecordId() {
    return resourceUrl;
  }
}
