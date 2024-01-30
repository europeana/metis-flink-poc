package eu.europeana.cloud.flink.common.tuples;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

@Table(keyspace = "flink_poc", name = "results")
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class NotificationTuple {

  @Column(name = "task_id")
  private long taskId;

  @Column(name = "resource")
  private String resource;

  @Column(name = "result_resource")
  private String resultResource;

  @Column(name = "info_text")
  @Builder.Default
  private String infoText= "";

  private String jobName;

  private boolean markedAsDeleted;


  //Temporary it is converted to row algouhght it shoiuld work autmatically.
  public Row toRow() {
    Row row = Row.withNames(RowKind.INSERT);
    row.setField("task_id", taskId);
    row.setField("resource", resource);
    row.setField("result_resource", resultResource);
    row.setField("info_text", infoText);
    return row;
  }

  public Tuple4<Long, String, String, String> toGenericTuple() {
    return new Tuple4<>(taskId, resource, resultResource, infoText);
  }
}
