package eu.europeana.cloud.source;

import eu.europeana.cloud.model.DataPartition;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.flink.api.connector.source.SourceEvent;


@AllArgsConstructor
@Getter
public class SplitCompletedEvent implements SourceEvent {

  private final DataPartition currentSplit;

}
