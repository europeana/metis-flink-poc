package eu.europeana.cloud.flink.common.tuples;

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
public class RecordTuple {

  private String recordId;
  private byte[] fileContent;
  @Builder.Default
  private String errorMessage = "";
}
