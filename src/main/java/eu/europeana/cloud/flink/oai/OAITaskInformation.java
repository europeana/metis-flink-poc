package eu.europeana.cloud.flink.oai;

import eu.europeana.cloud.flink.common.TaskInformation;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvest;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@Getter
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class OAITaskInformation extends TaskInformation  {

  private OaiHarvest oaiHarvest;
  private String metisDatasetId;

  public String getDatasetId() {
    return metisDatasetId;
  }
}
