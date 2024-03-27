package eu.europeana.cloud.flink.simpledb;

import eu.europeana.cloud.flink.common.tuples.RecordTuple;
import java.nio.charset.StandardCharsets;
import org.apache.flink.api.common.functions.MapFunction;

public class DbEntityToTupleConvertingOperator implements MapFunction<RecordExecutionEntity, RecordTuple> {

  @Override
  public RecordTuple map(RecordExecutionEntity record) throws Exception {
    return RecordTuple.builder()
                      .recordId(record.getRecordId())
                      .fileContent(record.getRecordData().getBytes(StandardCharsets.UTF_8))
                      .build();
  }
}
