package eu.europeana.cloud.tool.validation;

import eu.europeana.cloud.tool.JobName;

public class JobParamValidatorFactory {


    private JobParamValidatorFactory() {
    }

    public static JobParamValidator getValidator(String jobName) {
        switch (jobName) {
            case JobName.TRANSFORMATION -> {
                return new TransformationJobParamValidator();
            }
            case JobName.VALIDATION_EXTERNAL, JobName.VALIDATION_INTERNAL -> {
                return new ValidationJobParamValidator();
            }
            case JobName.NORMALIZATION -> {
                return new NormalizationJobParamValidator();
            }
            case JobName.ENRICHMENT -> {
                return new EnrichmentJobParamValidator();
            }
            case JobName.MEDIA -> {
                return new MediaJobParamValidator();
            }
            case JobName.INDEXING -> {
                return new IndexingJobParamValidator();
            }
            case JobName.OAI_HARVEST -> {
                return new OAIJobParamValidator();
            }
            default -> throw new IllegalArgumentException("No validator for: " + jobName);
        }
    }
}
