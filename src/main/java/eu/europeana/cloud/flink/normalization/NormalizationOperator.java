package eu.europeana.cloud.flink.normalization;

import eu.europeana.cloud.flink.common.tuples.FileTuple;
import eu.europeana.normalization.Normalizer;
import eu.europeana.normalization.NormalizerFactory;
import eu.europeana.normalization.model.NormalizationResult;
import java.nio.charset.StandardCharsets;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

public class NormalizationOperator extends RichMapFunction<FileTuple, FileTuple> {


  private transient NormalizerFactory normalizerFactory;

  @Override
  public FileTuple map(FileTuple fileTuple) throws Exception {
    if (fileTuple.getDpsRecord().isMarkedAsDeleted()){
      return fileTuple;
    }

    final Normalizer normalizer = normalizerFactory.getNormalizer();
    String document = new String(fileTuple.getFileContent(), StandardCharsets.UTF_8);

    NormalizationResult normalizationResult = normalizer.normalize(document);
    if (normalizationResult.getErrorMessage() != null) {
      throw new RuntimeException(
          "Unable to normalize file: " + fileTuple.getFileUrl() + " - " + normalizationResult.getErrorMessage());
    }

    String outputXml = normalizationResult.getNormalizedRecordInEdmXml();
   // Thread.sleep(10000);
    return fileTuple.toBuilder().fileContent(outputXml.getBytes(StandardCharsets.UTF_8)).build();
  }

  public void open(Configuration parameters) {
    normalizerFactory = new NormalizerFactory();
  }
}
