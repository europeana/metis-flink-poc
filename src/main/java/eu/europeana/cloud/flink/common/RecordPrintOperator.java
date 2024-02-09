package eu.europeana.cloud.flink.common;

import eu.europeana.cloud.flink.common.tuples.RecordTuple;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordPrintOperator implements MapFunction<RecordTuple, RecordTuple> {

  private static final Logger LOGGER = LoggerFactory.getLogger(RecordPrintOperator.class);

  @Override
  public RecordTuple map(RecordTuple tuple) throws Exception {
    LOGGER.info("Tuple: {}", tuple);
    return tuple;
  }
}
