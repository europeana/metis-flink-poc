package eu.europeana.cloud.flink.common.tuples;

import lombok.Builder;
import lombok.Value;
import org.apache.flink.util.ExceptionUtils;

@Value
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
