package eu.europeana.cloud.operator;

import static eu.europeana.cloud.flink.client.constants.postgres.JobName.OAI_HARVEST;
import static eu.europeana.cloud.flink.client.constants.postgres.JobParamName.DATASET_ID;
import static eu.europeana.cloud.flink.client.constants.postgres.JobParamName.METADATA_PREFIX;
import static eu.europeana.cloud.flink.client.constants.postgres.JobParamName.OAI_REPOSITORY_URL;


import eu.europeana.cloud.model.ExecutionRecord;
import eu.europeana.cloud.model.ExecutionRecord.ExecutionRecordBuilder;
import eu.europeana.cloud.model.ExecutionRecordKey;
import eu.europeana.cloud.model.ExecutionRecordResult;
import eu.europeana.cloud.model.ExecutionRecordResult.ExecutionRecordResultBuilder;
import eu.europeana.cloud.flink.client.constants.postgres.JobParamName;
import eu.europeana.metis.harvesting.HarvesterFactory;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvester;
import eu.europeana.metis.harvesting.oaipmh.OaiRecord;
import eu.europeana.metis.harvesting.oaipmh.OaiRecordHeader;
import eu.europeana.metis.harvesting.oaipmh.OaiRepository;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordHarvestingOperator extends ProcessFunction<OaiRecordHeader, ExecutionRecordResult> {

  private static final Logger LOGGER = LoggerFactory.getLogger(RecordHarvestingOperator.class);

  private static final int DEFAULT_RETRIES = 3;
  private static final int SLEEP_TIME = 5000;

  private transient OaiHarvester harvester;

  private ParameterTool parameterTool;

  public RecordHarvestingOperator(ParameterTool parameterTool) {
    this.parameterTool = parameterTool;
  }

  @Override
  public void processElement(OaiRecordHeader header,
      ProcessFunction<OaiRecordHeader, ExecutionRecordResult>.Context ctx, Collector<ExecutionRecordResult> out) {
    String externalRecordId = header.getOaiIdentifier();
    ExecutionRecordKey key = ExecutionRecordKey.builder().datasetId(parameterTool.get(DATASET_ID))
                                               .executionId(parameterTool.get(JobParamName.TASK_ID))
                                               .recordId(externalRecordId).build();

    ExecutionRecordBuilder executionRecordBuilder = ExecutionRecord.builder().executionRecordKey(key).executionName(OAI_HARVEST);
    ExecutionRecordResultBuilder executionRecordResultBuilder = ExecutionRecordResult.builder();
    try {
      OaiRecord oaiRecord = harvestRecordsContent(header);
      executionRecordBuilder.recordData(new String(oaiRecord.getContent().readAllBytes(), StandardCharsets.UTF_8));
    } catch (Exception e) {
      executionRecordBuilder.recordData("");
      executionRecordResultBuilder.exception(ExceptionUtils.stringifyException(e));
    }
    executionRecordResultBuilder.executionRecord(executionRecordBuilder.build());
    out.collect(executionRecordResultBuilder.build());
  }

  private OaiRecord harvestRecordsContent(OaiRecordHeader header) throws Exception {
    Instant harvestingStartTime = Instant.now();
    String recordId = header.getOaiIdentifier();
    LOGGER.debug("Starting harvesting for: {}", recordId);

    if (recordId == null) {
      throw new NullPointerException("Records id is null!");
    }

    String endpointLocation = parameterTool.getRequired(OAI_REPOSITORY_URL);
    String metadataPrefix = parameterTool.getRequired(METADATA_PREFIX);


    var oaiRecord = harvester.harvestRecord(new OaiRepository(endpointLocation, metadataPrefix), recordId);
    LOGGER.debug("Harvesting finished in: {}ms for {}", millisecondsSince(harvestingStartTime), recordId);
    return oaiRecord;
  }

  @Override
  public void open(Configuration parameters) {
    harvester = HarvesterFactory.createOaiHarvester(null, DEFAULT_RETRIES, SLEEP_TIME);
  }

  public static long millisecondsSince(Instant start) {
    return Instant.now().toEpochMilli() - start.toEpochMilli();
  }
}
