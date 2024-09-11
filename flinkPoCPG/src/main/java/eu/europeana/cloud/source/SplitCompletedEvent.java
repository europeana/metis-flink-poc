package eu.europeana.cloud.source;

import eu.europeana.cloud.model.DataPartition;
import lombok.Value;
import org.apache.flink.api.connector.source.SourceEvent;

/**
 * Event meaning that given split was completed by reader and all the record are saved in the DB.
 */
@Value
public class SplitCompletedEvent implements SourceEvent {

  long checkpointId;
  DataPartition split;
  int completedRecords;

}
