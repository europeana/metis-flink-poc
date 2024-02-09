package eu.europeana.cloud.flink.oai.harvest;

import eu.europeana.cloud.flink.common.tuples.HarvestedRecordTuple;
import eu.europeana.cloud.flink.oai.OAITaskInformation;
import eu.europeana.cloud.flink.common.tuples.RecordTuple;
import eu.europeana.metis.transformation.service.EuropeanaGeneratedIdsMap;
import eu.europeana.metis.transformation.service.EuropeanaIdCreator;
import eu.europeana.metis.transformation.service.EuropeanaIdException;
import java.nio.charset.StandardCharsets;
import org.apache.flink.api.common.functions.MapFunction;

public class IdAssigningOperator implements MapFunction<HarvestedRecordTuple, RecordTuple> {

  private final OAITaskInformation taskInformation;

  public IdAssigningOperator(OAITaskInformation taskInformation) {
    this.taskInformation=taskInformation;
  }

  @Override
  public RecordTuple map(HarvestedRecordTuple tuple) throws Exception {
    EuropeanaGeneratedIdsMap europeanaIdentifier = getEuropeanaIdentifier(tuple);
//    tuple.addParameter(PluginParameterKeys.ADDITIONAL_LOCAL_IDENTIFIER, europeanaIdentifier.getSourceProvidedChoAbout());
//    tuple.addParameter(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER, europeanaIdentifier.getEuropeanaGeneratedId());
    return RecordTuple.builder()
        .europeanaId(europeanaIdentifier.getEuropeanaGeneratedId())
        .fileContent(tuple.getFileContent())
        .timestamp(tuple.getTimestamp())
                      .build();
  }

  private EuropeanaGeneratedIdsMap getEuropeanaIdentifier(HarvestedRecordTuple tuple)
      throws EuropeanaIdException {
    String document = new String(tuple.getFileContent(), StandardCharsets.UTF_8);
    EuropeanaIdCreator europeanIdCreator = new EuropeanaIdCreator();
    return europeanIdCreator.constructEuropeanaId(document, taskInformation.getMetisDatasetId());
  }
}
