package eu.europeana.cloud.flink.common;

import lombok.Getter;
import org.apache.flink.api.java.utils.ParameterTool;

@Getter
public class JobParameters<T> {

  private final ParameterTool tool;
  private  final T taskParameters;

  public JobParameters(ParameterTool tool, T taskParameters) {
    this.tool = tool;
    this.taskParameters = taskParameters;
  }
}
