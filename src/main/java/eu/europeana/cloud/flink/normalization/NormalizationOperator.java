package eu.europeana.cloud.flink.normalization;

import eu.europeana.cloud.flink.common.FollowingJobMainOperator;
import eu.europeana.cloud.flink.common.tuples.RecordTuple;
import eu.europeana.normalization.Normalizer;
import eu.europeana.normalization.NormalizerFactory;
import eu.europeana.normalization.model.NormalizationResult;
import eu.europeana.normalization.util.NormalizationConfigurationException;
import eu.europeana.normalization.util.NormalizationException;
import java.nio.charset.StandardCharsets;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NormalizationOperator extends FollowingJobMainOperator {

  private static final Logger LOGGER = LoggerFactory.getLogger(NormalizationOperator.class);
  private transient NormalizerFactory normalizerFactory;

  @Override
  public RecordTuple map(RecordTuple tuple) {
    try {
      String outputXml = getNormalizedRecord(tuple);
      return RecordTuple.builder().recordId(tuple.getRecordId())
                        .fileContent(outputXml.getBytes(StandardCharsets.UTF_8))
                        .build();
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      return RecordTuple.builder()
                        .recordId(tuple.getRecordId())
                        .fileContent(tuple.getFileContent())
                        .errorMessage(e.getMessage())
                        .build();
    }
  }

  private String getNormalizedRecord(RecordTuple tuple) throws NormalizationConfigurationException, NormalizationException {
    final Normalizer normalizer = normalizerFactory.getNormalizer();
    String document = new String(tuple.getFileContent(), StandardCharsets.UTF_8);
    NormalizationResult normalizationResult = normalizer.normalize(document);
    if (normalizationResult.getErrorMessage() != null) {
      throw new RuntimeException(
          "Unable to normalize file: " + tuple.getRecordId() + " - " + normalizationResult.getErrorMessage());
    }
    return normalizationResult.getNormalizedRecordInEdmXml();
  }

  @Override
  public void open(Configuration parameters) {
      normalizerFactory = new NormalizerFactory();
      LOGGER.info("Created normalization operator.");
  }

}
