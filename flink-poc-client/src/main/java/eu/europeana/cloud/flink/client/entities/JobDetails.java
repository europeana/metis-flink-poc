package eu.europeana.cloud.flink.client.entities;

import lombok.Data;

@Data
public class JobDetails {

  private String jid;
  private String name;
  private String state;

}
