package eu.europeana.cloud.oai.harvest;

import java.time.Instant;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class HarvestedRecordTuple {

  private String externalId;
  private Instant timestamp;
  private byte[] fileContent;
}
