package eu.europeana.cloud.flink.oai.harvest;

import static eu.europeana.cloud.flink.common.FollowingJobMainOperator.ERROR_STREAM_TAG;

import eu.europeana.cloud.flink.common.tuples.ErrorTuple;
import eu.europeana.cloud.flink.common.tuples.HarvestedRecordTuple;
import eu.europeana.cloud.flink.oai.OAITaskParams;
import eu.europeana.cloud.flink.common.tuples.RecordTuple;
import eu.europeana.metis.transformation.service.EuropeanaGeneratedIdsMap;
import eu.europeana.metis.transformation.service.EuropeanaIdCreator;
import eu.europeana.metis.transformation.service.EuropeanaIdException;
import java.nio.charset.StandardCharsets;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IdAssigningOperator extends ProcessFunction<HarvestedRecordTuple, RecordTuple> {

  private static final Logger LOGGER = LoggerFactory.getLogger(IdAssigningOperator.class);

  private final OAITaskParams taskParams;

  public IdAssigningOperator(OAITaskParams taskParams) {
    this.taskParams = taskParams;
  }

  @Override
  public void processElement(HarvestedRecordTuple tuple, ProcessFunction<HarvestedRecordTuple, RecordTuple>.Context ctx,
      Collector<RecordTuple> out) {
    try {
      out.collect(assigneEuropeanaIdentifier(tuple));
    } catch (Exception e) {
      LOGGER.warn("Error while evaluating Europeana Id, for record with external id: {}",
          tuple.getExternalId(), e);
      ctx.output(ERROR_STREAM_TAG, ErrorTuple.builder()
                                             .recordId("/externalIdentifier/" + tuple.getExternalId())
                                             .exception(e)
                                             .build());
    }
  }

  private RecordTuple assigneEuropeanaIdentifier(HarvestedRecordTuple tuple) throws Exception {
    // //Uncomment for error handling tests
    //    if (tuple.getExternalId().equals("ecloud_e2e_tests_NLS____NLS2__RS_643______06VMZI9_sr")) {
    //      throw new XMLParseException("Bad XML!");
    //    }

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
