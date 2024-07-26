package eu.europeana.cloud.source.oai;

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
