package eu.europeana.cloud.flink.common.tuples;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class TaskRecordTuple {

  private TaskParams taskParams;
  private RecordParams recordParams;

  public boolean isMarkedAsDeleted() {
    return recordParams.isMarkedAsDeleted();
  }

  public String getFileUrl() {
    return recordParams.getResourceUrl();
  }
}
