package eu.europeana.cloud.flink.integration.ecloud.normalization;

import eu.europeana.cloud.flink.common.tuples.FileTuple;
import eu.europeana.cloud.flink.common.tuples.RecordTuple;
import eu.europeana.cloud.flink.normalization.NormalizationOperator;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ECloudNormalizationOperator extends RichMapFunction<FileTuple, FileTuple> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ECloudNormalizationOperator.class);

  private transient NormalizationOperator operator;

  @Override
  public FileTuple map(FileTuple inputTuple) throws Exception {
    if (inputTuple.isMarkedAsDeleted()) {
      return inputTuple;
    }

    RecordTuple result = operator.map(
        RecordTuple.builder().recordId(inputTuple.getResourceUrl()).fileContent(inputTuple.getFileContent()).build());

    return inputTuple.toBuilder().fileContent(result.getFileContent()).build();
  }

  public void open(Configuration parameters) {
    operator = new NormalizationOperator();
    operator.open(parameters);
    LOGGER.info("Created ECloudNormalization operator.");
  }
}
