package eu.europeana.cloud.flink.oai.source;

import java.io.IOException;
import java.time.Instant;
import java.util.Date;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OAIEnumeratorStateSerializer  implements SimpleVersionedSerializer<OAIEnumeratorState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(OAIEnumeratorStateSerializer.class);
  public static final int CURRENT_VERSION = 0;
  public OAIEnumeratorStateSerializer() {
    LOGGER.info("Initializing OAIEnumeratorStateSerializer");
  }

  @Override
  public int getVersion() {
    return CURRENT_VERSION;
  }

  @Override
  public byte[] serialize(OAIEnumeratorState oaiEnumeratorState) throws IOException {
    return new byte[0];
  }

  @Override
  public OAIEnumeratorState deserialize(int i, byte[] bytes) throws IOException {
    return new OAIEnumeratorState();
  }
}
