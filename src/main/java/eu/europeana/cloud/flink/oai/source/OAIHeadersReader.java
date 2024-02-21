package eu.europeana.cloud.flink.oai.source;

import eu.europeana.cloud.flink.oai.OAITaskParams;
import eu.europeana.metis.harvesting.HarvesterFactory;
import eu.europeana.metis.harvesting.ReportingIteration.IterationResult;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvester;
import eu.europeana.metis.harvesting.oaipmh.OaiRecordHeader;
import eu.europeana.metis.harvesting.oaipmh.OaiRecordHeaderIterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OAIHeadersReader implements SourceReader<OaiRecordHeader, OAISplit> {

  private static final Logger LOGGER = LoggerFactory.getLogger(OAIHeadersReader.class);

  private static final int DEFAULT_RETRIES = 3;
  private static final int SLEEP_TIME = 5000;
  private final SourceReaderContext context;
  private final OAITaskParams taskParams;
  private boolean active;
  private CompletableFuture<Void> available = new CompletableFuture();

  public OAIHeadersReader(SourceReaderContext context, OAITaskParams taskParams) {
    this.context = context;
    this.taskParams = taskParams;
    LOGGER.info("Created oai reader.");
  }

  @Override
  public void start() {
    LOGGER.info("Started oai reader.");
  }

  @Override
  public InputStatus pollNext(ReaderOutput<OaiRecordHeader> output) throws Exception {
    LOGGER.info("Executed poll: active: {}", active);
    if (!active) {
      available = new CompletableFuture<>();
      context.sendSplitRequest();
      return InputStatus.NOTHING_AVAILABLE;
    }

    OaiHarvester harvester = HarvesterFactory.createOaiHarvester(null, DEFAULT_RETRIES, SLEEP_TIME);

    OaiRecordHeaderIterator headerIterator = harvester.harvestRecordHeaders(taskParams.getOaiHarvest());
    headerIterator.forEach(oaiHeader -> {
      output.collect(oaiHeader);
      //      try {
      //        Thread.sleep(3000);
      //      } catch (InterruptedException e) {
      //        throw new RuntimeException(e);
      //      }
      return IterationResult.CONTINUE;
    });
    headerIterator.close();
    active = false;
    available = new CompletableFuture<>();
    return InputStatus.END_OF_INPUT;
  }

  @Override
  public List<OAISplit> snapshotState(long checkpointId) {
    LOGGER.info("Snapshotted state: {}", checkpointId);
    return null;
  }

  @Override
  public CompletableFuture<Void> isAvailable() {
    return available;
  }

  @Override
  public void addSplits(List<OAISplit> splits) {
    LOGGER.info("Adding split");
    active = true;
    available.complete(null);
    LOGGER.info("Added split");
  }


  @Override
  public void notifyNoMoreSplits() {
    LOGGER.info("Notified: no more splits");
  }

  @Override
  public void close() throws Exception {
    LOGGER.info("close");
    //No needed for now
  }
}
