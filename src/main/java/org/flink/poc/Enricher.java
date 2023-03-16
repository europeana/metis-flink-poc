package org.flink.poc;

import eu.europeana.enrichment.rest.client.EnrichmentWorker;
import eu.europeana.enrichment.rest.client.EnrichmentWorkerImpl;
import eu.europeana.enrichment.rest.client.dereference.DereferencerProvider;
import eu.europeana.enrichment.rest.client.enrichment.EnricherProvider;
import eu.europeana.enrichment.rest.client.exceptions.DereferenceException;
import eu.europeana.enrichment.rest.client.exceptions.EnrichmentException;
import eu.europeana.enrichment.rest.client.report.ProcessedResult;

public class Enricher {

  private final String dereferenceURL;
  private final String enrichmentEntityManagementUrl;
  private final String enrichmentEntityApiUrl;
  private final String enrichmentEntityApiKey;
  private transient EnrichmentWorker enrichmentWorker;

  public Enricher(String dereferenceURL, String enrichmentEntityManagementUrl, String enrichmentEntityApiUrl,
      String enrichmentEntityApiKey) {
    this.dereferenceURL = dereferenceURL;
    this.enrichmentEntityManagementUrl = enrichmentEntityManagementUrl;
    this.enrichmentEntityApiUrl = enrichmentEntityApiUrl;
    this.enrichmentEntityApiKey = enrichmentEntityApiKey;
    prepareEnrichmentWorker();
  }

  public ProcessedResult<String> enrich(String fileContent) {
    return this.enrichmentWorker.process(fileContent);
  }

  private void prepareEnrichmentWorker() {
    final EnricherProvider enricherProvider = new EnricherProvider();
    enricherProvider.setEnrichmentPropertiesValues(enrichmentEntityManagementUrl, enrichmentEntityApiUrl, enrichmentEntityApiKey);
    final DereferencerProvider dereferencerProvider = new DereferencerProvider();
    dereferencerProvider.setDereferenceUrl(dereferenceURL);
    dereferencerProvider.setEnrichmentPropertiesValues(enrichmentEntityManagementUrl, enrichmentEntityApiUrl,
        enrichmentEntityApiKey);
    try {
      enrichmentWorker = new EnrichmentWorkerImpl(dereferencerProvider.create(),
          enricherProvider.create());
    } catch (DereferenceException | EnrichmentException e) {
      throw new RuntimeException("Could not instantiate EnrichmentBolt due Exception in enrich worker creating", e);
    }
  }
}
