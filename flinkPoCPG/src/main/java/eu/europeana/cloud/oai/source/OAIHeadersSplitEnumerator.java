package eu.europeana.cloud.oai.source;

import java.io.IOException;
import java.util.List;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OAIHeadersSplitEnumerator implements SplitEnumerator<OAISplit, Void> {

  private static final Logger LOGGER = LoggerFactory.getLogger(OAIHeadersSplitEnumerator.class);
  private final SplitEnumeratorContext<OAISplit> context;
  private boolean finished;

  public OAIHeadersSplitEnumerator(SplitEnumeratorContext<OAISplit> context) {
    this.context = context;
  }

  @Override
  public void start() {
    LOGGER.info("Started split enumerator.");

  }

  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
    LOGGER.info("Handling split request, subtaskId: {}, host: {}");
    if (!finished) {
      context.assignSplit(new OAISplit(), subtaskId);
      LOGGER.info("Assigned split for subtaskId: {}, host: {}");
    }else{
      LOGGER.info("There are no more splits to assign, for subtaskId: {}, host: {}");
    }
    finished = true;
  }

  @Override
  public void addSplitsBack(List<OAISplit> splits, int subtaskId) {
    finished = false;
  }

  @Override
  public void addReader(int subtaskId) {
    LOGGER.info("Added reader for subtaskId: {}", subtaskId);
  }

  @Override
  public Void snapshotState(long checkpointId) throws Exception {
    //No needed for now
    return null;
  }

  @Override
  public void close() throws IOException {
    //No needed for now
  }
}
