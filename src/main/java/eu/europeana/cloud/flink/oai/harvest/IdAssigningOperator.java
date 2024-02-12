package eu.europeana.cloud.flink.oai.harvest;

import eu.europeana.cloud.flink.common.tuples.HarvestedRecordTuple;
import eu.europeana.cloud.flink.oai.OAITaskParams;
import eu.europeana.cloud.flink.common.tuples.RecordTuple;
import eu.europeana.metis.transformation.service.EuropeanaGeneratedIdsMap;
import eu.europeana.metis.transformation.service.EuropeanaIdCreator;
import eu.europeana.metis.transformation.service.EuropeanaIdException;
import java.nio.charset.StandardCharsets;
import org.apache.flink.api.common.functions.MapFunction;

public class IdAssigningOperator implements MapFunction<HarvestedRecordTuple, RecordTuple> {

  private final OAITaskParams taskParams;

  public IdAssigningOperator(OAITaskParams taskParams) {
    this.taskParams = taskParams;
  }

  @Override
  public RecordTuple map(HarvestedRecordTuple tuple) throws Exception {
    EuropeanaGeneratedIdsMap europeanaIdentifier = getEuropeanaIdentifier(tuple);
    //    tuple.addParameter(PluginParameterKeys.ADDITIONAL_LOCAL_IDENTIFIER, europeanaIdentifier.getSourceProvidedChoAbout());
    //    tuple.addParameter(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER, europeanaIdentifier.getEuropeanaGeneratedId());
    String europeanaId = europeanaIdentifier.getEuropeanaGeneratedId();
    return RecordTuple.builder()
                      .recordId(europeanaId)
                      .fileContent(tuple.getFileContent())
                      .build();
  }

  private EuropeanaGeneratedIdsMap getEuropeanaIdentifier(HarvestedRecordTuple tuple)
      throws EuropeanaIdException {
    String document = new String(tuple.getFileContent(), StandardCharsets.UTF_8);
    EuropeanaIdCreator europeanIdCreator = new EuropeanaIdCreator();
    return europeanIdCreator.constructEuropeanaId(document, taskParams.getMetisDatasetId());
  }
}
