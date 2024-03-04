package eu.europeana.cloud.flink.clustertest;

import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SleepOperator implements MapFunction<Long, String> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterTestJob.class);

  @Override
  public String map(Long value) throws Exception {
    LOGGER.info("Working on value: {} .....", value);
    Thread.sleep(5000L);
    LOGGER.info("Completed value: {}", value);
    return "slept for " + value;
  }
}
