package eu.europeana.cloud.flink.workflow;

import static eu.europeana.cloud.flink.common.JobsParametersConstants.CONFIGURATION_FILE_PATH;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.DATASET_ID;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.EXECUTION_ID;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.INDEXING_PROPERTIES_FILE_PATH;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.METADATA_PREFIX;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.OAI_REPOSITORY_URL;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.PREVIOUS_STEP_ID;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.ROOT_LOCATION;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.SCHEMATRON_LOCATION;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.SCHEMA_NAME;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.SET_SPEC;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.TARGET_INDEXING_DATABASE;
import static eu.europeana.cloud.flink.common.JobsParametersConstants.XSLT_URL;

import eu.europeana.cloud.flink.enrichment.EnrichmentJob;
import eu.europeana.cloud.flink.indexing.IndexingJob;
import eu.europeana.cloud.flink.media.MediaJob;
import eu.europeana.cloud.flink.normalization.NormalizationJob;
import eu.europeana.cloud.flink.oai.OAIJob;
import eu.europeana.cloud.flink.validation.ValidationJob;
import eu.europeana.cloud.flink.workflow.entities.SubmitJobRequest;
import eu.europeana.cloud.flink.xslt.XsltJob;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvest;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.JobExecutionResult;

public class StepFactories {

  public static final FollowingJobRequestFactory[] LIST = {
      new ExternalValidationStepFactory(),
      new XsltStepFactory(),
      new InternalValidationStepFactory(),
      new NormalizationStepFactory(),
      new EnrichmentStepFactory(),
      new MediaStepFactory(),
      new IndexingStepFactory(TargetIndexingDatabase.PREVIEW),
      new IndexingStepFactory(TargetIndexingDatabase.PUBLISH)
  };

  private static final String JOBS_CONFIG_PATH = "/jobs-config";

  public static SubmitJobRequest createOAIRequest(String datasetId, UUID executionId, OaiHarvest oaiHarvest, int parallelism) {
    return SubmitJobRequest.builder()
                           .entryClass(OAIJob.class.getName())
                           .parallelism(String.valueOf(parallelism))
                           .programArgs(prepareOaiArgs(datasetId, executionId, oaiHarvest))
                           .build();
  }

  private static Map<String, Object> prepareCommonArgs(String datasetId, UUID executionId, UUID previousSetId) {
    return new HashMap<>(Map.of(
        DATASET_ID, datasetId,
        EXECUTION_ID, executionId,
        PREVIOUS_STEP_ID, previousSetId));
  }

  private static Map<String, Object> prepareOaiArgs(String datasetId, UUID executionId, OaiHarvest oaiHarvest) {
    return Map.of(
        CONFIGURATION_FILE_PATH, JOBS_CONFIG_PATH + "/oai_job.properties",
        DATASET_ID, datasetId,
        EXECUTION_ID, executionId,
        SET_SPEC, oaiHarvest.getSetSpec(),
        METADATA_PREFIX, oaiHarvest.getMetadataPrefix(),
        OAI_REPOSITORY_URL, oaiHarvest.getRepositoryUrl());
  }

  public static abstract class FollowingJobRequestFactory {

    public SubmitJobRequest createRequest(String datasetId, UUID executionId, UUID previousSetId, int parallelism) {
      Map<String, Object> args = prepareCommonArgs(datasetId, executionId, previousSetId);
      args.putAll(prepareSpecificArgs());
      return SubmitJobRequest.builder()
                             .entryClass(getJobClass().getName())
                             .parallelism(String.valueOf(parallelism))
                             .programArgs(args)
                             .build();
    }

    protected abstract Class getJobClass();

    protected abstract Map<String, Object> prepareSpecificArgs();

  }

  public static class ExternalValidationStepFactory extends FollowingJobRequestFactory {

    @Override
    protected Class getJobClass() {
      return ValidationJob.class;
    }

    @Override
    protected Map<String, Object> prepareSpecificArgs() {
      return Map.of(
          CONFIGURATION_FILE_PATH, JOBS_CONFIG_PATH + "/validation_job.properties",
          SCHEMA_NAME, "http://ftp.eanadev.org/schema_zips/europeana_schemas-20231130.zip",
          ROOT_LOCATION, "EDM.xsd",
          SCHEMATRON_LOCATION, "schematron/schematron.xsl");
    }
  }

  public static class InternalValidationStepFactory extends FollowingJobRequestFactory {

    @Override
    protected Class getJobClass() {
      return ValidationJob.class;
    }

    @Override
    protected Map<String, Object> prepareSpecificArgs() {
      return Map.of(
          CONFIGURATION_FILE_PATH, JOBS_CONFIG_PATH + "/validation_job.properties",
          SCHEMA_NAME, "http://ftp.eanadev.org/schema_zips/europeana_schemas-20231130.zip",
          ROOT_LOCATION, "EDM-INTERNAL.xsd",
          SCHEMATRON_LOCATION, "schematron/schematron-internal.xsl");
    }
  }

  public static class IndexingStepFactory extends FollowingJobRequestFactory {

    private final TargetIndexingDatabase targetIndexingDatabase;

    public IndexingStepFactory(TargetIndexingDatabase targetIndexingDatabase) {
      this.targetIndexingDatabase = targetIndexingDatabase;
    }

    @Override
    protected Class getJobClass() {
      return IndexingJob.class;
    }

    @Override
    protected Map<String, Object> prepareSpecificArgs() {
      return Map.of(
          CONFIGURATION_FILE_PATH, JOBS_CONFIG_PATH + "/indexing_job.properties",
          INDEXING_PROPERTIES_FILE_PATH, JOBS_CONFIG_PATH + "/indexing.properties",
          TARGET_INDEXING_DATABASE, targetIndexingDatabase);
    }
  }

  public static class XsltStepFactory extends FollowingJobRequestFactory {

    @Override
    protected Class getJobClass() {
      return XsltJob.class;
    }

    @Override
    protected Map<String, Object> prepareSpecificArgs() {
      return Map.of(
          CONFIGURATION_FILE_PATH, JOBS_CONFIG_PATH + "/xslt_job.properties",
          XSLT_URL, "https://metis-core-rest.test.eanadev.org/datasets/xslt/default");
    }
  }

  private static class NormalizationStepFactory extends FollowingJobRequestFactory {

    @Override
    protected Class getJobClass() {
      return NormalizationJob.class;
    }

    @Override
    protected Map<String, Object> prepareSpecificArgs() {
      return Map.of(
          CONFIGURATION_FILE_PATH, JOBS_CONFIG_PATH + "/normalization_job.properties");
    }
  }

  private static class EnrichmentStepFactory extends FollowingJobRequestFactory {

    @Override
    protected Class getJobClass() {
      return EnrichmentJob.class;
    }

    @Override
    protected Map<String, Object> prepareSpecificArgs() {
      return Map.of(
          CONFIGURATION_FILE_PATH, JOBS_CONFIG_PATH + "/enrichment_job.properties");
    }

  }

  private static class MediaStepFactory extends FollowingJobRequestFactory {

    @Override
    protected Class getJobClass() {
      return MediaJob.class;
    }

    @Override
    protected Map<String, Object> prepareSpecificArgs() {
      return Map.of(
          CONFIGURATION_FILE_PATH, JOBS_CONFIG_PATH + "/media_job.properties");
    }

  }
}
