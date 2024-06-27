package eu.europeana.cloud.operator;

import eu.europeana.cloud.model.ExecutionRecord;
import eu.europeana.cloud.model.ExecutionRecordKey;
import eu.europeana.cloud.model.ExecutionRecordResult;
import eu.europeana.cloud.tool.JobName;
import eu.europeana.cloud.tool.JobParamName;
import eu.europeana.metis.mediaprocessing.*;
import eu.europeana.metis.mediaprocessing.exception.MediaExtractionException;
import eu.europeana.metis.mediaprocessing.exception.RdfDeserializationException;
import eu.europeana.metis.mediaprocessing.exception.RdfSerializationException;
import eu.europeana.metis.mediaprocessing.model.EnrichedRdf;
import eu.europeana.metis.mediaprocessing.model.RdfResourceEntry;
import eu.europeana.metis.mediaprocessing.model.ResourceExtractionResult;
import eu.europeana.metis.mediaprocessing.model.Thumbnail;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.List;

import static java.util.Objects.nonNull;

public class MediaOperator extends ProcessFunction<ExecutionRecord, ExecutionRecordResult> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MediaOperator.class);

    private ParameterTool parameterTool;
    private transient RdfDeserializer rdfDeserializer;
    private transient RdfSerializer rdfSerializer;
    private transient MediaExtractor mediaExtractor;

    @Override
    public void open(Configuration parameters) throws Exception {
        parameterTool = ParameterTool.fromMap(getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap());

        final RdfConverterFactory rdfConverterFactory = new RdfConverterFactory();
        rdfDeserializer = rdfConverterFactory.createRdfDeserializer();
        rdfSerializer = rdfConverterFactory.createRdfSerializer();
        final MediaProcessorFactory mediaProcessorFactory = new MediaProcessorFactory();
        mediaExtractor = mediaProcessorFactory.createMediaExtractor();
    }


    @Override
    public void processElement(ExecutionRecord sourceExecutionRecord, ProcessFunction<ExecutionRecord, ExecutionRecordResult>.Context ctx, Collector<ExecutionRecordResult> out) throws Exception {
        final byte[] rdfBytes = sourceExecutionRecord.getRecordData().getBytes(Charset.defaultCharset());
        final EnrichedRdf enrichedRdf;
        enrichedRdf = getEnrichedRdf(rdfBytes);

        RdfResourceEntry resourceMainThumbnail = rdfDeserializer.getMainThumbnailResourceForMediaExtraction(rdfBytes);
        boolean hasMainThumbnail = false;
        if (resourceMainThumbnail != null) {
            hasMainThumbnail = processResourceWithoutThumbnail(resourceMainThumbnail,
                    sourceExecutionRecord.getExecutionRecordKey().getRecordId(), enrichedRdf, mediaExtractor);
        }
        List<RdfResourceEntry> remainingResourcesList = rdfDeserializer.getRemainingResourcesForMediaExtraction(rdfBytes);
        if (hasMainThumbnail) {
            remainingResourcesList.forEach(entry ->
                    processResourceWithThumbnail(entry, sourceExecutionRecord.getExecutionRecordKey().getRecordId(), enrichedRdf,
                            mediaExtractor)
            );
        } else {
            remainingResourcesList.forEach(entry ->
                    processResourceWithoutThumbnail(entry, sourceExecutionRecord.getExecutionRecordKey().getRecordId(), enrichedRdf,
                            mediaExtractor)
            );
        }
        final byte[]
                outputRdfBytes = getOutputRdf(enrichedRdf);

        out.collect(
                ExecutionRecordResult.from(
                        sourceExecutionRecord,
                        parameterTool.get(JobParamName.TASK_ID),
                        JobName.MEDIA,
                        new String(outputRdfBytes),
                        null)
        );
    }

    private EnrichedRdf getEnrichedRdf(byte[] rdfBytes) throws RdfDeserializationException {
        return rdfDeserializer.getRdfForResourceEnriching(rdfBytes);
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

    private boolean processResourceWithThumbnail(RdfResourceEntry resourceToProcess, String recordId,
                                                 EnrichedRdf rdfForEnrichment, MediaExtractor extractor) {
        return processResource(resourceToProcess, recordId, rdfForEnrichment, extractor, true);
    }

    private void storeThumbnails(String recordId, List<Thumbnail> thumbnails) {
        if (nonNull(thumbnails)) {
            LOGGER.debug("Fake storing thumbnail");
        }
    }

    private byte[] getOutputRdf(EnrichedRdf rdfForEnrichment) throws RdfSerializationException {
        return rdfSerializer.serialize(rdfForEnrichment);
    }

    @Override
    public void close() throws Exception {
        mediaExtractor.close();
    }
}
