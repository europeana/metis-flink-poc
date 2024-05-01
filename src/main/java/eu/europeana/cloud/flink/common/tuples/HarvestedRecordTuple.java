package eu.europeana.cloud.flink.common.tuples;

import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@AllArgsConstructor
@NoArgsConstructor
@Setter
@Getter
@ToString
@EqualsAndHashCode
@Builder
public class HarvestedRecordTuple {

  private String externalId;
  private Instant timestamp;
  private byte[] fileContent;
}
