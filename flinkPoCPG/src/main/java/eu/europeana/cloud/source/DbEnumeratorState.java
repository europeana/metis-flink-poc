package eu.europeana.cloud.source;

import eu.europeana.cloud.model.DataPartition;
import java.io.Serializable;
import java.util.List;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class DbEnumeratorState implements Serializable {
  private long recordsToBeProcessed;
  private long allPartitionCount;
  private long startedPartitionCount;
  private long finishedRecordCount;
  private long commitCount;
  private List<DataPartition> incompletePartitions;

}
