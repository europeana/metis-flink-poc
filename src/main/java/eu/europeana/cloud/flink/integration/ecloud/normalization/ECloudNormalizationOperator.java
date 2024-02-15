package eu.europeana.cloud.flink.integration.ecloud.normalization;

import eu.europeana.cloud.flink.common.tuples.FileTuple;
import eu.europeana.normalization.Normalizer;
import eu.europeana.normalization.NormalizerFactory;
import eu.europeana.normalization.model.NormalizationResult;
import java.nio.charset.StandardCharsets;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ECloudNormalizationOperator extends RichMapFunction<FileTuple, FileTuple> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ECloudNormalizationOperator.class);

  private transient NormalizerFactory normalizerFactory;

  @Override
  public FileTuple map(FileTuple inputTuple) throws Exception {
    if (inputTuple.isMarkedAsDeleted()) {
      return inputTuple;
    }

    final Normalizer normalizer = normalizerFactory.getNormalizer();
    String document = new String(inputTuple.getFileContent(), StandardCharsets.UTF_8);

    NormalizationResult normalizationResult = normalizer.normalize(document);
    if (normalizationResult.getErrorMessage() != null) {
      throw new RuntimeException(
          "Unable to normalize file: " + inputTuple.getResourceUrl() + " - " + normalizationResult.getErrorMessage());
    }

    String outputXml = normalizationResult.getNormalizedRecordInEdmXml();
    FileTuple resultTuple = inputTuple.toBuilder().fileContent(outputXml.getBytes(StandardCharsets.UTF_8)).build();
    LOGGER.debug("Normalized file: {}", inputTuple.getResourceUrl());
    return resultTuple;
  }

  public void open(Configuration parameters) {
    normalizerFactory = new NormalizerFactory();
    LOGGER.info("Created normalization operator.");
  }
}
