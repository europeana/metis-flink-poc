package eu.europeana.cloud.flink.oai.source;

import java.io.IOException;
import java.util.List;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OAISplitEnumerator implements SplitEnumerator<OAISplit, Void> {

  private static final Logger LOGGER = LoggerFactory.getLogger(OAISplitEnumerator.class);
  private final SplitEnumeratorContext<OAISplit> context;
  private boolean finished;

  public OAISplitEnumerator(SplitEnumeratorContext<OAISplit> context) {
    this.context = context;
  }

  @Override
  public void start() {
    LOGGER.info("Started split enumerator.");

  }

  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
    LOGGER.info("Split request subtaskId: {}, host: {}");
    if (!finished) {
      context.assignSplit(new OAISplit(), subtaskId);
    }
    finished = true;
  }

  @Override
  public void addSplitsBack(List<OAISplit> splits, int subtaskId) {
    finished = false;
  }

  @Override
  public void addReader(int subtaskId) {
    LOGGER.info("Add reader subtaskId:{}", subtaskId);
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
