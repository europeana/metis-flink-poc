package eu.europeana.cloud.flink.xslt;

import eu.europeana.cloud.flink.common.FollowingJobMainOperator;
import eu.europeana.cloud.flink.common.tuples.RecordTuple;
import eu.europeana.metis.transformation.service.EuropeanaGeneratedIdsMap;
import eu.europeana.metis.transformation.service.EuropeanaIdCreator;
import eu.europeana.metis.transformation.service.EuropeanaIdException;
import eu.europeana.metis.transformation.service.TransformationException;
import eu.europeana.metis.transformation.service.XsltTransformer;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XsltOperator extends FollowingJobMainOperator {

  private static final Logger LOGGER = LoggerFactory.getLogger(XsltOperator.class);
  private final XsltParams taskParams;

  public XsltOperator(XsltParams taskParams) {
    this.taskParams = taskParams;
  }

  @Override
  public RecordTuple map(RecordTuple tuple) throws Exception {
    final XsltTransformer xsltTransformer = prepareXsltTransformer();

    StringWriter writer =
        xsltTransformer.transform(tuple.getFileContent(), prepareEuropeanaGeneratedIdsMap(tuple));

    RecordTuple result = RecordTuple.builder()
                                    .recordId(tuple.getRecordId())
                                    .fileContent(writer.toString().getBytes(StandardCharsets.UTF_8))
                                    .build();
    LOGGER.info("Transformed file: {}", tuple.getRecordId());
    return result;
  }


  private XsltTransformer prepareXsltTransformer()
      throws TransformationException {
    return new XsltTransformer(taskParams.getXsltUrl(), taskParams.getMetisDatasetName(),
        taskParams.getMetisDatasetCountry(), taskParams.getMetisDatasetLanguage());
  }

  private EuropeanaGeneratedIdsMap prepareEuropeanaGeneratedIdsMap(RecordTuple recordTuple)
      throws EuropeanaIdException {
    String metisDatasetId = taskParams.getMetisDatasetId();
    //Prepare europeana identifiers
    EuropeanaGeneratedIdsMap europeanaGeneratedIdsMap = null;
    if (!StringUtils.isBlank(metisDatasetId)) {
      String fileDataString = new String(recordTuple.getFileContent(), StandardCharsets.UTF_8);
      EuropeanaIdCreator europeanIdCreator = new EuropeanaIdCreator();
      europeanaGeneratedIdsMap = europeanIdCreator
          .constructEuropeanaId(fileDataString, metisDatasetId);
    }
    return europeanaGeneratedIdsMap;
  }

}
