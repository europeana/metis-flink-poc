package eu.europeana.cloud.flink.model;

public record TaskInfo(long taskId, long commitCount, long writeCount) {

}
