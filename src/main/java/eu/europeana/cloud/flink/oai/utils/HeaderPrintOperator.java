package eu.europeana.cloud.flink.oai.utils;

import eu.europeana.metis.harvesting.oaipmh.OaiRecordHeader;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HeaderPrintOperator implements MapFunction<OaiRecordHeader, OaiRecordHeader> {

  private static final Logger LOGGER = LoggerFactory.getLogger(HeaderPrintOperator.class);

  @Override
  public OaiRecordHeader map(OaiRecordHeader header) throws Exception {
    LOGGER.info("Header: id: {}, timestamp: {}, deleted: {}",
        header.getOaiIdentifier(), header.getDatestamp(), header.isDeleted());
    return header;
  }
}
