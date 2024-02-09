package eu.europeana.cloud.flink.oai.harvest;

import eu.europeana.metis.harvesting.oaipmh.OaiRecordHeader;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * Filters out deleted records.
 */
public class DeletedOutFilter implements FilterFunction<OaiRecordHeader> {

  @Override
  public boolean filter(OaiRecordHeader value) {
    return !value.isDeleted();
  }
}
