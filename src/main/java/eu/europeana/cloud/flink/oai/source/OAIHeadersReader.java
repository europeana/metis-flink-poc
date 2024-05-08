package eu.europeana.cloud.flink.oai.source;

import eu.europeana.cloud.flink.oai.OAITaskParams;
import eu.europeana.metis.harvesting.HarvesterException;
import eu.europeana.metis.harvesting.HarvesterFactory;
import eu.europeana.metis.harvesting.ReportingIteration.IterationResult;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvester;
import eu.europeana.metis.harvesting.oaipmh.OaiRecordHeader;
import eu.europeana.metis.harvesting.oaipmh.OaiRecordHeaderIterator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
  private CompletableFuture<Void> available = new CompletableFuture<>();
  private List<OAISplit> splits=new ArrayList<>();
  BlockingDeque<OaiRecordHeader> deque=new LinkedBlockingDeque<>(100);
  private OAISplit harvestedSplit;
  private int count;

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
    LOGGER.info("Executed poll: assigned splits: {}", splits);
    if (splits.isEmpty()) {
      available = new CompletableFuture<>();
      context.sendSplitRequest();
      return InputStatus.NOTHING_AVAILABLE;
    }else {
      if (harvestedSplit == null) {
        harvestedSplit = splits.get(0);
        harvestSplitInBackground( harvestedSplit);
      }

      OaiRecordHeader header = deque.poll(10, TimeUnit.SECONDS);
      if(header!=null){
        Thread.sleep(1000L);
        output.collect(header);
        harvestedSplit.setRecordDone(count++);
        harvestedSplit.setLastCheckpointedHeader(header.getOaiIdentifier());
        return InputStatus.MORE_AVAILABLE;
      }else {
        harvestedSplit=null;
        splits.remove(0);
        return InputStatus.END_OF_INPUT;
      }

    }
  }

  private void harvestSplitInBackground(OAISplit split) throws HarvesterException, IOException {

    ExecutorService executor = Executors.newSingleThreadExecutor(runnable -> new Thread(runnable,"Reading OAI source"));
    executor.submit((Callable<Void>) () -> {

      OaiHarvester harvester = HarvesterFactory.createOaiHarvester(null, DEFAULT_RETRIES, SLEEP_TIME);
      OaiRecordHeaderIterator headerIterator = harvester.harvestRecordHeaders(taskParams.getOaiHarvest());

      headerIterator.forEach(oaiHeader -> {
        try {
          deque.put(oaiHeader);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        return IterationResult.CONTINUE;
      });
      headerIterator.close();
      return null;
    });
    executor.shutdown();
  }

  @Override
  public List<OAISplit> snapshotState(long checkpointId) {
    LOGGER.info("Snapshotted state: {}, {}", checkpointId, splits);
    return splits;
  }

  @Override
  public CompletableFuture<Void> isAvailable() {
    return available;
  }

  @Override
  public void addSplits(List<OAISplit> splits) {
    LOGGER.info("Adding splits: {}",splits);
    this.splits.addAll(splits);
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

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    LOGGER.error("notifyCheckpointComplete: {}", checkpointId);
  }
}
