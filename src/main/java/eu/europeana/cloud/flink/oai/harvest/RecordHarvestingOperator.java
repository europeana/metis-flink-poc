package eu.europeana.cloud.flink.oai.harvest;

import static eu.europeana.cloud.flink.common.FollowingJobMainOperator.ERROR_STREAM_TAG;

import eu.europeana.cloud.common.utils.Clock;
import eu.europeana.cloud.flink.common.tuples.ErrorTuple;
import eu.europeana.cloud.flink.common.tuples.HarvestedRecordTuple;
import eu.europeana.cloud.flink.oai.OAITaskParams;
import eu.europeana.metis.harvesting.HarvesterFactory;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvester;
import eu.europeana.metis.harvesting.oaipmh.OaiRecordHeader;
import eu.europeana.metis.harvesting.oaipmh.OaiRepository;
import java.time.Instant;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordHarvestingOperator extends ProcessFunction<OaiRecordHeader, HarvestedRecordTuple> {

  private static final Logger LOGGER = LoggerFactory.getLogger(RecordHarvestingOperator.class);

  private static final int DEFAULT_RETRIES = 3;
  private static final int SLEEP_TIME = 5000;
  private final OAITaskParams taskParams;

  private transient OaiHarvester harvester;


  public RecordHarvestingOperator(OAITaskParams taskParams) {
    this.taskParams = taskParams;
  }

  @Override
  public void processElement(OaiRecordHeader header, ProcessFunction<OaiRecordHeader, HarvestedRecordTuple>.Context ctx,
      Collector<HarvestedRecordTuple> out) {
    try {
      out.collect(harvestRecordsContent(header));
    } catch (Exception e) {
      LOGGER.warn("Error while harvesting record content from source for OAI identifier: {}",
          header.getOaiIdentifier(), e);
      ctx.output(ERROR_STREAM_TAG, ErrorTuple.builder()
                                             .recordId("/oaiIdentifier/" + header.getOaiIdentifier())
                                             .exception(e)
                                             .build());
    }
  }

  private HarvestedRecordTuple harvestRecordsContent(OaiRecordHeader header) throws Exception {
    //     //Uncomment for error handling tests
    //    if(header.getOaiIdentifier().equals("ecloud_e2e_tests_NLS____NLS2__RS_23_______0QIWEFD_sr")){
    //      throw new HttpConnectTimeoutException("Time passsed!");
    //    }

    Instant harvestingStartTime = Instant.now();
    String recordId = header.getOaiIdentifier();
    LOGGER.debug("Starting harvesting for: {}", recordId);

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

    LOGGER.debug("Harvesting finished in: {}ms for {}", Clock.millisecondsSince(harvestingStartTime), recordId);
    return result;
  }

  @Override
  public void open(Configuration parameters) {
    harvester = HarvesterFactory.createOaiHarvester(null, DEFAULT_RETRIES, SLEEP_TIME);
  }
}
