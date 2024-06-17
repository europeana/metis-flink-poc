package eu.europeana.cloud.oai.harvest;

import static eu.europeana.cloud.tool.JobParamName.METADATA_PREFIX;
import static eu.europeana.cloud.tool.JobParamName.OAI_REPOSITORY_URL;


import eu.europeana.metis.harvesting.HarvesterFactory;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvester;
import eu.europeana.metis.harvesting.oaipmh.OaiRecordHeader;
import eu.europeana.metis.harvesting.oaipmh.OaiRepository;
import java.time.Instant;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordHarvestingOperator extends ProcessFunction<OaiRecordHeader, HarvestedRecordTuple> {

  private static final Logger LOGGER = LoggerFactory.getLogger(RecordHarvestingOperator.class);

  private static final int DEFAULT_RETRIES = 3;
  private static final int SLEEP_TIME = 5000;

  private transient OaiHarvester harvester;

  private ParameterTool parameterTool;

  public RecordHarvestingOperator(ParameterTool parameterTool) {
    this.parameterTool = parameterTool;
  }

  @Override
  public void processElement(OaiRecordHeader header, ProcessFunction<OaiRecordHeader, HarvestedRecordTuple>.Context ctx,
      Collector<HarvestedRecordTuple> out) throws Exception {
      out.collect(harvestRecordsContent(header));
  }

  private HarvestedRecordTuple harvestRecordsContent(OaiRecordHeader header) throws Exception {
    Instant harvestingStartTime = Instant.now();
    String recordId = header.getOaiIdentifier();
    LOGGER.debug("Starting harvesting for: {}", recordId);

    if (recordId == null) {
      throw new NullPointerException("Records id is null!");
    }

    String endpointLocation = parameterTool.getRequired(OAI_REPOSITORY_URL);
    String metadataPrefix = parameterTool.getRequired(METADATA_PREFIX);

    var oaiRecord = harvester.harvestRecord(new OaiRepository(endpointLocation, metadataPrefix), recordId);

    HarvestedRecordTuple result = HarvestedRecordTuple.builder()
                                                      .externalId(recordId)
                                                      .timestamp(oaiRecord.getHeader().getDatestamp())
                                                      .fileContent(oaiRecord.getRecord().readAllBytes())
                                                      .build();

    LOGGER.debug("Harvesting finished in: {}ms for {}", millisecondsSince(harvestingStartTime), recordId);
    return result;
  }

  @Override
  public void open(Configuration parameters) {
    harvester = HarvesterFactory.createOaiHarvester(null, DEFAULT_RETRIES, SLEEP_TIME);
  }

  public static long millisecondsSince(Instant start) {
    return Instant.now().toEpochMilli() - start.toEpochMilli();
  }
}
