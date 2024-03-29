package eu.europeana.cloud.flink.common.tuples;


import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class FileTuple {
  private TaskParams taskParams;
  private RecordParams recordParams;
  private byte[] fileContent;

  public String getResourceUrl() {
    return recordParams.getResourceUrl();
  }

  public boolean isMarkedAsDeleted() {
    return recordParams.isMarkedAsDeleted();
  }
}
