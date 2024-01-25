package eu.europeana.cloud.flink.common.tuples;


import eu.europeana.cloud.copieddependencies.DpsRecord;
import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class FileTuple {

  private DpsRecord dpsRecord;
  private byte[] fileContent;

  public String getFileUrl() {
    return dpsRecord.getRecordId();
  }
}
