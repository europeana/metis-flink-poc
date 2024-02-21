package eu.europeana.cloud.flink.common;

import eu.europeana.cloud.flink.common.tuples.ErrorTuple;
import eu.europeana.cloud.flink.common.tuples.RecordTuple;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class FollowingJobMainOperator extends ProcessFunction<RecordTuple, RecordTuple> {

  public static final OutputTag<ErrorTuple> ERROR_STREAM_TAG = new OutputTag<>("error-stream") {
  };
  private static final Logger LOGGER = LoggerFactory.getLogger(FollowingJobMainOperator.class);

  @Override
  public final void processElement(RecordTuple tuple,
      ProcessFunction<RecordTuple, RecordTuple>.Context context, Collector<RecordTuple> out) {
    try {
      out.collect(map(tuple));
      LOGGER.debug("Processed record, id: {}", tuple.getRecordId());
    } catch (Exception e) {
      LOGGER.warn("{} error: {}", getClass().getName(), tuple.getRecordId(), e);
      context.output(ERROR_STREAM_TAG, ErrorTuple.builder()
                                                 .recordId(tuple.getRecordId())
                                                 .exception(e)
                                                 .build());
    }
  }

  public abstract RecordTuple map(RecordTuple tuple) throws Exception;
}
