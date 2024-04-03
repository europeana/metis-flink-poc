package eu.europeana.cloud.flink.media;

import static java.util.Objects.nonNull;

import eu.europeana.cloud.flink.common.FollowingJobMainOperator;
import eu.europeana.cloud.flink.common.tuples.RecordTuple;
import eu.europeana.enrichment.rest.client.exceptions.DereferenceException;
import eu.europeana.enrichment.rest.client.exceptions.EnrichmentException;
import eu.europeana.metis.mediaprocessing.MediaExtractor;
import eu.europeana.metis.mediaprocessing.MediaProcessorFactory;
import eu.europeana.metis.mediaprocessing.RdfConverterFactory;
import eu.europeana.metis.mediaprocessing.RdfDeserializer;
import eu.europeana.metis.mediaprocessing.RdfSerializer;
import eu.europeana.metis.mediaprocessing.exception.MediaExtractionException;
import eu.europeana.metis.mediaprocessing.exception.MediaProcessorException;
import eu.europeana.metis.mediaprocessing.exception.RdfDeserializationException;
import eu.europeana.metis.mediaprocessing.exception.RdfSerializationException;
import eu.europeana.metis.mediaprocessing.model.EnrichedRdf;
import eu.europeana.metis.mediaprocessing.model.RdfResourceEntry;
import eu.europeana.metis.mediaprocessing.model.ResourceExtractionResult;
import eu.europeana.metis.mediaprocessing.model.Thumbnail;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MediaOperator extends FollowingJobMainOperator {

  private static final Logger LOGGER = LoggerFactory.getLogger(MediaOperator.class);

  private transient MediaExtractor mediaExtractor;
  private transient RdfSerializer rdfSerializer;
  private transient RdfDeserializer rdfDeserializer;

  @Override
  public RecordTuple map(RecordTuple tuple) throws Exception {
    final byte[] rdfBytes = tuple.getFileContent();
    final EnrichedRdf enrichedRdf;
    enrichedRdf = getEnrichedRdf(rdfBytes);

    RdfResourceEntry resourceMainThumbnail = rdfDeserializer.getMainThumbnailResourceForMediaExtraction(rdfBytes);
    boolean hasMainThumbnail = false;
    if (resourceMainThumbnail != null) {
      hasMainThumbnail = processResourceWithoutThumbnail(resourceMainThumbnail,
          tuple.getRecordId(), enrichedRdf, mediaExtractor);
    }
    List<RdfResourceEntry> remainingResourcesList = rdfDeserializer.getRemainingResourcesForMediaExtraction(rdfBytes);
    if (hasMainThumbnail) {
      remainingResourcesList.forEach(entry ->
          processResourceWithThumbnail(entry, tuple.getRecordId(), enrichedRdf,
              mediaExtractor)
      );
    } else {
      remainingResourcesList.forEach(entry ->
          processResourceWithoutThumbnail(entry, tuple.getRecordId(), enrichedRdf,
              mediaExtractor)
      );
    }
    final byte[]
    outputRdfBytes = getOutputRdf(enrichedRdf);

    return RecordTuple.builder().recordId(tuple.getRecordId())
        .fileContent(outputRdfBytes)
                      .build();
  }

  private EnrichedRdf getEnrichedRdf(byte[] rdfBytes) throws RdfDeserializationException {
    return rdfDeserializer.getRdfForResourceEnriching(rdfBytes);
  }

  private byte[] getOutputRdf(EnrichedRdf rdfForEnrichment) throws RdfSerializationException {
    return rdfSerializer.serialize(rdfForEnrichment);
  }

  private boolean processResourceWithThumbnail(RdfResourceEntry resourceToProcess, String recordId,
      EnrichedRdf rdfForEnrichment, MediaExtractor extractor) {
    return processResource(resourceToProcess, recordId, rdfForEnrichment, extractor, true);
  }

  private boolean processResourceWithoutThumbnail(RdfResourceEntry resourceToProcess, String recordId,
      EnrichedRdf rdfForEnrichment, MediaExtractor extractor) {
    return processResource(resourceToProcess, recordId, rdfForEnrichment, extractor, false);
  }

  private boolean processResource(RdfResourceEntry resourceToProcess, String recordId,
      EnrichedRdf rdfForEnrichment, MediaExtractor extractor, boolean gotMainThumbnail) {
    ResourceExtractionResult extraction;
    boolean successful = false;

    try {
      // Perform media extraction
      extraction = extractor.performMediaExtraction(resourceToProcess, gotMainThumbnail);

      // Check if extraction for media was successful
      successful = extraction != null;

      // If successful then store data
      if (successful) {
        rdfForEnrichment.enrichResource(extraction.getMetadata());
        if (!CollectionUtils.isEmpty(extraction.getThumbnails())) {
          storeThumbnails(recordId, extraction.getThumbnails());
        }
      }

    } catch (MediaExtractionException e) {
      LOGGER.warn("Error while extracting media for record {}. ", recordId, e);
    }

    return successful;
  }

  private void storeThumbnails(String recordId, List<Thumbnail> thumbnails) {
    if (nonNull(thumbnails)) {
      LOGGER.debug("Fake storing thumbnail");
    }
  }

  public void open(Configuration parameters) throws DereferenceException, EnrichmentException, MediaProcessorException {
    final RdfConverterFactory rdfConverterFactory = new RdfConverterFactory();
    rdfDeserializer = rdfConverterFactory.createRdfDeserializer();
    rdfSerializer = rdfConverterFactory.createRdfSerializer();
    final MediaProcessorFactory mediaProcessorFactory = new MediaProcessorFactory();
    mediaExtractor = mediaProcessorFactory.createMediaExtractor();
  }

  @Override
  public void close() throws Exception {
    mediaExtractor.close();
  }
}
