package eu.europeana.cloud.flink.enrichment;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.DEREFERENCE_SERVICE_URL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.ENRICHMENT_ENTITY_API_KEY;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.ENRICHMENT_ENTITY_API_URL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.ENRICHMENT_ENTITY_MANAGEMENT_URL;

import eu.europeana.cloud.flink.common.FollowingJobMainOperator;
import eu.europeana.cloud.flink.common.tuples.RecordTuple;
import eu.europeana.enrichment.rest.client.EnrichmentWorker;
import eu.europeana.enrichment.rest.client.EnrichmentWorkerImpl;
import eu.europeana.enrichment.rest.client.dereference.DereferencerProvider;
import eu.europeana.enrichment.rest.client.enrichment.EnricherProvider;
import eu.europeana.enrichment.rest.client.exceptions.DereferenceException;
import eu.europeana.enrichment.rest.client.exceptions.EnrichmentException;
import eu.europeana.enrichment.rest.client.report.ProcessedResult;
import eu.europeana.enrichment.rest.client.report.ProcessedResult.RecordStatus;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnrichmentOperator extends FollowingJobMainOperator {

  private static final Logger LOGGER = LoggerFactory.getLogger(EnrichmentOperator.class);
  private final String dereferenceURL;
  private final String enrichmentEntityManagementUrl;
  private final String enrichmentEntityApiUrl;
  private final String enrichmentEntityApiKey;
  private transient EnrichmentWorker enrichmentWorker;


  public EnrichmentOperator(Properties properties) {
    this.dereferenceURL = properties.getProperty(DEREFERENCE_SERVICE_URL, "http://dereference.com");
    this.enrichmentEntityManagementUrl = properties.getProperty(ENRICHMENT_ENTITY_MANAGEMENT_URL,
        "http://entity-management-url.com");
    this.enrichmentEntityApiUrl = properties.getProperty(ENRICHMENT_ENTITY_API_URL, "http://entity-api-url.com");
    this.enrichmentEntityApiKey = properties.getProperty(ENRICHMENT_ENTITY_API_KEY, "some-key");
  }

  @Override
  public RecordTuple map(RecordTuple tuple) {
    try {
      ProcessedResult<String> enrichmentResult =
          enrichmentWorker.process(new String(tuple.getFileContent(), StandardCharsets.UTF_8));
      if (enrichmentResult.getRecordStatus() != RecordStatus.CONTINUE) {
        String reportString = enrichmentResult.getReport().stream().map(Object::toString).collect(Collectors.joining("\n"));
        throw new RuntimeException("Enrichment ended with error!:\n" + reportString);
      }
      return RecordTuple.builder()
                        .recordId(tuple.getRecordId())
                        .fileContent(enrichmentResult.getProcessedRecord().getBytes(StandardCharsets.UTF_8))
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


  public void open(Configuration parameters) throws DereferenceException, EnrichmentException {
    final EnricherProvider enricherProvider = new EnricherProvider();
    enricherProvider.setEnrichmentPropertiesValues(enrichmentEntityManagementUrl, enrichmentEntityApiUrl, enrichmentEntityApiKey);
    final DereferencerProvider dereferencerProvider = new DereferencerProvider();
    dereferencerProvider.setDereferenceUrl(dereferenceURL);
    dereferencerProvider.setEnrichmentPropertiesValues(enrichmentEntityManagementUrl, enrichmentEntityApiUrl,
        enrichmentEntityApiKey);

    enrichmentWorker = new EnrichmentWorkerImpl(dereferencerProvider.create(), enricherProvider.create());
    LOGGER.info("Created enrichment operator.");
  }

}
