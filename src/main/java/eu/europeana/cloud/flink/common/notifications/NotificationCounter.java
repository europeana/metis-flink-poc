package eu.europeana.cloud.flink.common.notifications;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class NotificationCounter {

  private int processedRecordsCount;
  private int ignoredRecordsCount;
  private int deletedRecordsCount;
  private int processedErrorsCount;
  private int deletedErrorsCount;


  public static NotificationCounter aggregate(NotificationCounter c1, NotificationCounter c2) {
    NotificationCounter result = new NotificationCounter();
    result.processedRecordsCount = c1.processedRecordsCount + c2.processedRecordsCount;
    result.ignoredRecordsCount = c1.ignoredRecordsCount + c2.ignoredRecordsCount;
    result.deletedRecordsCount = c1.deletedRecordsCount + c2.deletedRecordsCount;
    result.processedErrorsCount = c1.processedErrorsCount + c2.processedErrorsCount;
    result.deletedErrorsCount = c1.deletedErrorsCount + c2.deletedErrorsCount;
    return result;
  }

  public int getProcessed() {
    return processedRecordsCount + ignoredRecordsCount + deletedRecordsCount;
  }
}
