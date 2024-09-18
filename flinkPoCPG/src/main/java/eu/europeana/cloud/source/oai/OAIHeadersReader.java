package eu.europeana.cloud.source.oai;

import static eu.europeana.cloud.flink.client.constants.postgres.JobParamName.METADATA_PREFIX;
import static eu.europeana.cloud.flink.client.constants.postgres.JobParamName.OAI_REPOSITORY_URL;
import static eu.europeana.cloud.flink.client.constants.postgres.JobParamName.SET_SPEC;

import eu.europeana.metis.harvesting.HarvesterFactory;
import eu.europeana.metis.harvesting.HarvestingIterator;
import eu.europeana.metis.harvesting.ReportingIteration.IterationResult;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvest;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvester;
import eu.europeana.metis.harvesting.oaipmh.OaiRecordHeader;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.io.InputStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OAIHeadersReader implements SourceReader<OaiRecordHeader, OAISplit> {

  private static final Logger LOGGER = LoggerFactory.getLogger(OAIHeadersReader.class);

  private static final int DEFAULT_RETRIES = 3;
  private static final int SLEEP_TIME = 5000;
  private final SourceReaderContext context;
  private final ParameterTool parameterTool;
  private boolean active;

  private CompletableFuture<Void> available = new CompletableFuture<>();
  private boolean completed;
  private OaiHarvester harvester;
  private OaiHarvest oaiHarvest;

  public OAIHeadersReader(SourceReaderContext context, ParameterTool parameterTool) {
    this.context = context;
    this.parameterTool = parameterTool;
    LOGGER.info("Created oai reader.");
  }

  @Override
  public void start() {
    LOGGER.info("Started oai reader.");
    harvester = HarvesterFactory.createOaiHarvester(null, DEFAULT_RETRIES, SLEEP_TIME);
    oaiHarvest = new OaiHarvest(
        parameterTool.getRequired(OAI_REPOSITORY_URL),
        parameterTool.getRequired(METADATA_PREFIX),
        parameterTool.getRequired(SET_SPEC));
  }

  @Override
  public InputStatus pollNext(ReaderOutput<OaiRecordHeader> output) throws Exception {
    if (completed) {
      LOGGER.info("Poll on completed OAI source.");
      return InputStatus.END_OF_INPUT;
    }
    LOGGER.info("Executed poll: active: {}", active);
    if (!active) {
      available = new CompletableFuture<>();
      context.sendSplitRequest();
      return InputStatus.NOTHING_AVAILABLE;
    }

    HarvestingIterator<OaiRecordHeader, OaiRecordHeader> headerIterator = harvester.harvestRecordHeaders(
        oaiHarvest);
    headerIterator.forEach(oaiHeader -> {
      output.collect(oaiHeader);
      return IterationResult.CONTINUE;
    });
    headerIterator.close();
    active = false;
    available = CompletableFuture.completedFuture(null);
    completed=true;

    LOGGER.info("Completed OAI source.");
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
    LOGGER.info("OAI - close");
    //No needed for now
  }
}
