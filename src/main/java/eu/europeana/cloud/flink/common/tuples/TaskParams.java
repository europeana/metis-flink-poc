package eu.europeana.cloud.flink.common.tuples;

import java.time.Instant;
import java.util.Date;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class TaskParams {

  private long id;

  private Date sentDate;
  private String providerId;
  private String dataSetId;
  private String newRepresentationName;
  private String outputMimeType;

  private String revisionName;
  private String revisionProviderId;
  private Date revisionCreationTimeStamp;

  public Instant getSentTimestamp() {
    return sentDate.toInstant();
  }

}
