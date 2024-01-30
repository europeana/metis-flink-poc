package eu.europeana.cloud.flink.common.tuples;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class WrittenRecordTuple {
  private TaskParams taskParams;
  private RecordParams recordParams;
  private String newResourceUrl;

  public long getTaskId() {
    return taskParams.getId();
  }

  public String getResourceUrl() {
    return recordParams.getResourceUrl();
  }
}
