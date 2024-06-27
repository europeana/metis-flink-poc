package eu.europeana.cloud.source.oai;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Data
@AllArgsConstructor
@Builder
public class OAIEnumeratorState {

  private static final Logger LOGGER = LoggerFactory.getLogger(OAIEnumeratorState.class);

  public OAIEnumeratorState() {
    LOGGER.info("Initializing OAIEnumeratorState");
  }

  private boolean splitAssigned;
}
