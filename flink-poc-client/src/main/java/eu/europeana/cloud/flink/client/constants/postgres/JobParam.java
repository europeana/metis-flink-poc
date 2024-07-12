package eu.europeana.cloud.flink.client.constants.postgres;

public class JobParam {

    public static final int DEFAULT_OPERATOR_PARALLELISM = 4;
    public static final int DEFAULT_READER_PARALLELISM = 4;
    public static final int DEFAULT_SINK_PARALLELISM = 4;
    public static final int DEFAULT_READER_MAX_RECORD_PENDING_COUNT = 100;

    private JobParam() {
    }
}
