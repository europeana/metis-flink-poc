package eu.europeana.cloud.flink.common.tuples;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.Value;
import org.apache.flink.util.ExceptionUtils;

@AllArgsConstructor
@NoArgsConstructor
@Setter
@Getter
@ToString
@EqualsAndHashCode
@Builder
public class ErrorTuple {

  private String recordId;

  private String exception;

  public static class ErrorTupleBuilder {

    //We could not store the exception itself cause of serialization error:
    //Caused by: java.io.NotSerializableException: net.sf.saxon.om.StructuredQName
    //So it needs to be converted to String.
    public ErrorTupleBuilder exception(Exception exception) {
      this.exception = ExceptionUtils.stringifyException(exception);
      return this;
    }
  }

}
