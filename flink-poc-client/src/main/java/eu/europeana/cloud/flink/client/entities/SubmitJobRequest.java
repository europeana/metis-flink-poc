package eu.europeana.cloud.flink.client.entities;


import java.util.Map;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class SubmitJobRequest {

  private String entryClass;
  private String parallelism;
  private String programArgs;
  private String savepointPath;
  private boolean allowNonRestoredState;

  public static class SubmitJobRequestBuilder {

    public SubmitJobRequestBuilder programArgs(Map<String, Object> argsMap) {
      this.programArgs = argsMap.entrySet().stream()
                                .map(entry -> "--" + entry.getKey() + " " + entry.getValue())
                                .collect(Collectors.joining(" "));
      return this;
    }
  }
}
