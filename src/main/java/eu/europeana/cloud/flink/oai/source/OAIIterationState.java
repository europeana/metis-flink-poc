package eu.europeana.cloud.flink.oai.source;

import java.util.UUID;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class OAIIterationState {
  private String datasetId;
  private UUID executionId;
  private int completedCount;
  private String lastIdentifier;
}
