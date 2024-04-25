package eu.europeana.cloud.flink.common;

import eu.europeana.cloud.flink.enrichment.EnrichmentJob;
import eu.europeana.cloud.flink.indexing.IndexingJob;
import eu.europeana.cloud.flink.media.MediaJob;
import eu.europeana.cloud.flink.normalization.NormalizationJob;
import eu.europeana.cloud.flink.oai.OAIJob;
import eu.europeana.cloud.flink.validation.ValidationJob;
import eu.europeana.cloud.flink.xslt.XsltJob;

public final class GenericJobFactory {

  private GenericJobFactory() {
  }

  public static GenericJob createJob(String className) {
    return switch (className) {
      case "eu.europeana.cloud.flink.oai.OAIJob" -> new OAIJob();
      case "eu.europeana.cloud.flink.validation.ValidationJob" -> new ValidationJob();
      case "eu.europeana.cloud.flink.xslt.XsltJob" -> new XsltJob();
      case "eu.europeana.cloud.flink.normalization.NormalizationJob" -> new NormalizationJob();
      case "eu.europeana.cloud.flink.media.MediaJob" -> new MediaJob();
      case "eu.europeana.cloud.flink.enrichment.EnrichmentJob" -> new EnrichmentJob();
      case "eu.europeana.cloud.flink.indexing.IndexingJob" -> new IndexingJob();
      default -> null;
    };
  }
}
