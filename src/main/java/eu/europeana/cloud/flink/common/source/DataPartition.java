package eu.europeana.cloud.flink.common.source;

import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;

public record DataPartition(long offset, long limit) implements SourceSplit, Serializable {

    @Override
    public String splitId() {
        return "customSplitId";
    }
}
