package eu.europeana.cloud.flink.oai.source;

import java.io.IOException;
import java.util.List;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OAIHeadersSplitEnumerator implements SplitEnumerator<OAISplit, OAIEnumeratorState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(OAIHeadersSplitEnumerator.class);
  private final SplitEnumeratorContext<OAISplit> context;
  private final OAIEnumeratorState state;
  private boolean finished;

  public OAIHeadersSplitEnumerator(SplitEnumeratorContext<OAISplit> context, OAIEnumeratorState state) {
    this.context = context;
    this.state = state == null ? new OAIEnumeratorState() : state;
  }

  @Override
  public void start() {
    LOGGER.info("Started split enumerator.");

  }

  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
    LOGGER.info("Handling split request, subtaskId: {}, host: {}", subtaskId, requesterHostname);
    if (!finished) {
      context.assignSplit(new OAISplit(), subtaskId);
      LOGGER.info("Assigned split for subtaskId: {}, host: {}", subtaskId, requesterHostname);
    } else {
      LOGGER.info("There are no more splits to assign, for subtaskId: {}, host: {}", subtaskId, requesterHostname);
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
  public OAIEnumeratorState snapshotState(long checkpointId) throws Exception {
    return state;
  }

  public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
    LOGGER.error("SourceEventHere. SubtaskId: {}, event: {}", subtaskId, sourceEvent);
  }

  @Override
  public void close() throws IOException {
    //No needed for now
  }
}
