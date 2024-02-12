package eu.europeana.cloud.flink.common;

import java.util.UUID;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@Getter
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class IntermediateTaskParams extends TaskParams {
  private UUID previousStepId;
}
