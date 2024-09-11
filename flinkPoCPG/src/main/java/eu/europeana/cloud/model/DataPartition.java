package eu.europeana.cloud.model;

import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;

public record DataPartition(long offset, long limit) implements SourceSplit, Serializable {

    //TODO Check if is it proper implementation.
    @Override
    public String splitId() {
        return "customSplitId";
    }
}
