package eu.europeana.cloud.operator;

import eu.europeana.cloud.model.ExecutionRecord;
import eu.europeana.cloud.model.ExecutionRecordKey;
import eu.europeana.cloud.model.ExecutionRecordResult;
import eu.europeana.metis.transformation.service.EuropeanaGeneratedIdsMap;
import eu.europeana.metis.transformation.service.EuropeanaIdCreator;
import eu.europeana.metis.transformation.service.EuropeanaIdException;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IdAssigningOperator extends ProcessFunction<ExecutionRecordResult, ExecutionRecordResult> {

  private static final Logger LOGGER = LoggerFactory.getLogger(IdAssigningOperator.class);

  @Override
  public void processElement(ExecutionRecordResult tuple,
      ProcessFunction<ExecutionRecordResult, ExecutionRecordResult>.Context ctx,
      Collector<ExecutionRecordResult> out)
      throws Exception {
    ExecutionRecordResult result;
    if (tuple.getException() == null) {
      String europeanaId = assigneEuropeanaIdentifier(tuple);
      ExecutionRecordKey keyWithExternalId = tuple.getExecutionRecord().getExecutionRecordKey();
      ExecutionRecordKey keyWithEuropeanaId = ExecutionRecordKey
          .builder()
          .executionId(keyWithExternalId.getExecutionId())
          .datasetId(keyWithExternalId.getDatasetId())
          .recordId(europeanaId)
          .build();
      result = ExecutionRecordResult.from(ExecutionRecord.builder()
                                                         .recordData(tuple.getRecordData())
                                                         .executionName(tuple.getExecutionRecord().getExecutionName())
                                                         .executionRecordKey(keyWithEuropeanaId)
                                                         .build());
    } else {
      result = tuple;
    }
    out.collect(result);
  }

  private String assigneEuropeanaIdentifier(ExecutionRecordResult tuple) throws Exception {
    EuropeanaGeneratedIdsMap europeanaIdentifier = getEuropeanaIdentifier(tuple);
    String europeanaId = europeanaIdentifier.getEuropeanaGeneratedId();
    LOGGER.debug("Assigned Europeana id: {}, for external record: {}", europeanaId, tuple.getRecordId());
    return europeanaId;
  }

  private EuropeanaGeneratedIdsMap getEuropeanaIdentifier(ExecutionRecordResult tuple)
      throws EuropeanaIdException {
    EuropeanaIdCreator europeanIdCreator = new EuropeanaIdCreator();
    return europeanIdCreator.constructEuropeanaId(tuple.getRecordData(),
        tuple.getExecutionRecord().getExecutionRecordKey().getDatasetId());
  }
}
