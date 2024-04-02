package eu.europeana.cloud.flink.indexing;

import static eu.europeana.cloud.flink.common.utils.JobUtils.readProperties;

import eu.europeana.cloud.flink.common.tuples.RecordTuple;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import eu.europeana.metis.transformation.service.TransformationException;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexerTester {
  private static final Logger LOGGER = LoggerFactory.getLogger(IndexerTester.class);
  public static void main(String[] args) throws Exception {
    IndexingTaskParams params =
        IndexingTaskParams.builder()
                          .indexingProperties(readProperties(
                              "/home/marcin/IdeaProjects/ApacheFlinkPoC/config/jobs/indexing.properties"))
            .metisDatasetId("4")
            .database(TargetIndexingDatabase.PREVIEW)
                                      .build();
    IndexingOperator operator=new IndexingOperator(params);
    operator.open(null);
    operator.map(RecordTuple.builder().build());
    LOGGER.info("Done!");
    operator.close();
  }
}
