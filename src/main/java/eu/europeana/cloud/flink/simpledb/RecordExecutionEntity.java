package eu.europeana.cloud.flink.simpledb;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Table(keyspace = "flink_poc", name = "EXECUTION_RECORD")
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class RecordExecutionEntity {

    @Column(name = "DATASET_ID")
    private String datasetId;

    @Column(name = "EXECUTION_ID")
    private String executionId;

    @Column(name = "EXECUTION_NAME")
    private String executionName;

    @Column(name = "RECORD_ID")
    private String recordId;

    @Column(name = "RECORD_DATA")
    private String recordData;

}
