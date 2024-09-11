package eu.europeana.cloud.source;

import eu.europeana.cloud.model.DataPartition;
import lombok.Value;
import org.apache.flink.api.connector.source.SourceEvent;

/**
 * Event with progress snapshot was created on a reader - number of record emitted by reader to execution.
 * The event does not mean that records are already stored in DB, but contains chekpointId, so could be
 * held and used when given checkpoint is completed.
 */
@Value
public class ProgressSnapshotEvent implements SourceEvent {
  long checkpointId;
  DataPartition split;
  int emittedRecordCount;
}
