package eu.europeana.cloud.flink.common.tuples;

import java.time.Instant;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class RecordTuple {

  private String europeanaId;
  private Instant timestamp;
  private byte[] fileContent;

}
