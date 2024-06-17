package eu.europeana.cloud.oai.harvest;

import static eu.europeana.cloud.tool.JobName.OAI_HARVEST;
import static eu.europeana.cloud.tool.JobParamName.DATASET_ID;
import static eu.europeana.cloud.tool.JobParamName.EXECUTION_ID;
import eu.europeana.cloud.model.ExecutionRecord;
import eu.europeana.cloud.model.ExecutionRecordKey;
import eu.europeana.cloud.model.ExecutionRecordResult;
import eu.europeana.metis.transformation.service.EuropeanaGeneratedIdsMap;
import eu.europeana.metis.transformation.service.EuropeanaIdCreator;
import eu.europeana.metis.transformation.service.EuropeanaIdException;
import java.nio.charset.StandardCharsets;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IdAssigningOperator extends ProcessFunction<HarvestedRecordTuple, ExecutionRecordResult> {

  private static final Logger LOGGER = LoggerFactory.getLogger(IdAssigningOperator.class);

  private final ParameterTool parameterTool;

  public IdAssigningOperator(ParameterTool parameterTool) {
    this.parameterTool = parameterTool;
  }

  @Override
  public void processElement(HarvestedRecordTuple tuple, ProcessFunction<HarvestedRecordTuple, ExecutionRecordResult>.Context ctx,
      Collector<ExecutionRecordResult> out) throws Exception {
    String europeanaId = assigneEuropeanaIdentifier(tuple);
    ExecutionRecordKey key = ExecutionRecordKey.builder().datasetId(parameterTool.get(DATASET_ID))
                                               .executionId(parameterTool.get(EXECUTION_ID))
                                               .recordId(europeanaId).build();
    ExecutionRecord executionRecord = ExecutionRecord.builder().executionRecordKey(key).recordData(
                                                         new String(tuple.getFileContent(), StandardCharsets.UTF_8))
                                                     .executionName(OAI_HARVEST).build();
    ExecutionRecordResult result = ExecutionRecordResult.builder().executionRecord(executionRecord).build();
    out.collect(result);
  }

  private String assigneEuropeanaIdentifier(HarvestedRecordTuple tuple) throws Exception {
    EuropeanaGeneratedIdsMap europeanaIdentifier = getEuropeanaIdentifier(tuple);
    String europeanaId = europeanaIdentifier.getEuropeanaGeneratedId();
    LOGGER.debug("Assigned Europeana id: {}, for external record: {}", europeanaId, tuple.getExternalId());
    return europeanaId;
  }

  private EuropeanaGeneratedIdsMap getEuropeanaIdentifier(HarvestedRecordTuple tuple)
      throws EuropeanaIdException {
    String document = new String(tuple.getFileContent(), StandardCharsets.UTF_8);
    EuropeanaIdCreator europeanIdCreator = new EuropeanaIdCreator();
    return europeanIdCreator.constructEuropeanaId(document, parameterTool.get(DATASET_ID));
  }
}
