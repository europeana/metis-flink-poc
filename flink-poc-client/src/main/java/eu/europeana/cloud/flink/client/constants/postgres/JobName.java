package eu.europeana.cloud.flink.client.constants.postgres;

public class JobName {

    public static final String VALIDATION_INTERNAL = "VALIDATION_INTERNAL";
    public static final String VALIDATION_EXTERNAL = "VALIDATION_EXTERNAL";
    public static final String TRANSFORMATION = "TRANSFORMATION";
    public static final String NORMALIZATION = "NORMALIZATION";
    public static final String ENRICHMENT = "ENRICHMENT";
    public static final String MEDIA = "MEDIA";
    public static final String INDEXING = "INDEXING";
    public static final String OAI_HARVEST = "OAI_HARVEST";

    private JobName() {
    }
}
