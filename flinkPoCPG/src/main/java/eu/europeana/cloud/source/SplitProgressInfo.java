package eu.europeana.cloud.source;

import lombok.Data;
import org.apache.flink.api.connector.source.SourceEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class stores information about progress on the given split (partition).
 */
@Data
public class SplitProgressInfo {

  private static final Logger LOGGER = LoggerFactory.getLogger(SplitProgressInfo.class);

  /**
   * Records of the split (partition) emitted before given checkpoint
   */
  private int emittedRecordCount = 0;

  private long checkpointId;

  public int update(ProgressSnapshotEvent event) {
    int increase = evalProgressIncrease(event, event.getEmittedRecordCount());
    checkpointId = event.getCheckpointId();
    emittedRecordCount = event.getEmittedRecordCount();
    return increase;
  }

  public long update(SplitCompletedEvent event) {
    int increase = evalProgressIncrease(event, event.getCompletedRecords());
    checkpointId = event.getCheckpointId();
    emittedRecordCount = event.getCompletedRecords();
    return increase;
  }


  private int evalProgressIncrease(SourceEvent event, int emittedRecordFromEvent) {
    int increase = emittedRecordFromEvent - emittedRecordCount;
    if (increase < 0) {
      throw new SourceConsistencyException(
          "Split progress gone backward! Current emitted record count: " + emittedRecordCount + " event: " + event);
    }
    return increase;
  }
}
