package eu.europeana.cloud.flink.oai.harvest;

import eu.europeana.cloud.common.utils.Clock;
import eu.europeana.cloud.flink.common.tuples.HarvestedRecordTuple;
import eu.europeana.cloud.flink.oai.OAITaskParams;
import eu.europeana.metis.harvesting.HarvesterFactory;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvester;
import eu.europeana.metis.harvesting.oaipmh.OaiRecordHeader;
import eu.europeana.metis.harvesting.oaipmh.OaiRepository;
import java.time.Instant;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordHarvestingOperator extends RichMapFunction<OaiRecordHeader, HarvestedRecordTuple> {

  private static final Logger LOGGER = LoggerFactory.getLogger(RecordHarvestingOperator.class);

  private static final int DEFAULT_RETRIES = 3;
  private static final int SLEEP_TIME = 5000;
  private final OAITaskParams taskParams;

  private transient OaiHarvester harvester;


  public RecordHarvestingOperator(OAITaskParams taskParams) {
    this.taskParams = taskParams;
  }

  @Override
  public HarvestedRecordTuple map(OaiRecordHeader header) throws Exception {
    Instant harvestingStartTime = Instant.now();
    String recordId = header.getOaiIdentifier();
    LOGGER.info("Starting harvesting for: {}", recordId);

    if (recordId == null) {
      throw new NullPointerException("Records id is null!");
    }

    String endpointLocation = taskParams.getOaiHarvest().getRepositoryUrl();
    String metadataPrefix = taskParams.getOaiHarvest().getMetadataPrefix();

    var oaiRecord = harvester.harvestRecord(new OaiRepository(endpointLocation, metadataPrefix), recordId);

    HarvestedRecordTuple result = HarvestedRecordTuple.builder()
                                                      .externalId(recordId)
                                                      .timestamp(oaiRecord.getHeader().getDatestamp())
                                                      .fileContent(oaiRecord.getRecord().readAllBytes())
                                                      .build();

    LOGGER.info("Harvesting finished in: {}ms for {}", Clock.millisecondsSince(harvestingStartTime), recordId);
    return result;
  }

  @Override
  public void open(Configuration parameters) {
    harvester = HarvesterFactory.createOaiHarvester(null, DEFAULT_RETRIES, SLEEP_TIME);
  }
}
