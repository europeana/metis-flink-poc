package eu.europeana.cloud.flink.common;

import eu.europeana.cloud.flink.common.tuples.FileTuple;
import eu.europeana.cloud.flink.common.tuples.NotificationTuple;
import org.apache.flink.api.common.functions.RichMapFunction;

public class NotificationOperator extends RichMapFunction<FileTuple, NotificationTuple> {

  @Override
  public NotificationTuple map(FileTuple fileTuple) {


    return NotificationTuple.builder()
                            .taskId(fileTuple.getDpsRecord().getTaskId())
                            .resource(fileTuple.getFileUrl())
                            .infoText("OK")
                            .build();
  }
}
