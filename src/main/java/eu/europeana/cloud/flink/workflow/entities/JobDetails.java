package eu.europeana.cloud.flink.workflow.entities;

import lombok.Data;

@Data
public class JobDetails {

  private String jid;
  private String name;
  private String state;

}
