package eu.europeana.cloud.flink.common.mcs;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.MCS_URL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.TOPOLOGY_USER_NAME;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.TOPOLOGY_USER_PASSWORD;

import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.flink.common.tuples.NotificationTuple;
import eu.europeana.cloud.flink.common.tuples.NotificationTuple.NotificationTupleBuilder;
import eu.europeana.cloud.flink.common.tuples.WrittenRecordTuple;
import eu.europeana.cloud.mcs.driver.RevisionServiceClient;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import java.net.MalformedURLException;
import java.util.Properties;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AddToRevisionOperator extends RichMapFunction<WrittenRecordTuple, NotificationTuple> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AddToRevisionOperator.class);
  private final Properties properties;
  private transient RevisionServiceClient revisionsClient;

  public AddToRevisionOperator(Properties properties) {
    this.properties = properties;
  }

  @Override
  public NotificationTuple map(WrittenRecordTuple tuple) throws Exception {
    NotificationTupleBuilder result = NotificationTuple.builder()
                                                       .taskId(tuple.getTaskId())
                                                       .resource(tuple.getResourceUrl());
    Revision outputRevision = prepareRevisionEntity(tuple);

    if (tuple.getRecordParams().isMarkedAsDeleted()) {
      outputRevision.setDeleted(true);
      addRevision(tuple.getResourceUrl(), outputRevision);
      result.markedAsDeleted(true);
    } else {
      addRevision(tuple.getNewResourceUrl(), outputRevision);
      result.resultResource(tuple.getNewResourceUrl());
    }

    return result.build();
  }

  private void addRevision(String resourceUrl, Revision outputRevision) throws MalformedURLException, MCSException {
    final UrlParser urlParser = new UrlParser(resourceUrl);
    revisionsClient.addRevision(
        urlParser.getPart(UrlPart.RECORDS),
        urlParser.getPart(UrlPart.REPRESENTATIONS),
        urlParser.getPart(UrlPart.VERSIONS),
        outputRevision);
    LOGGER.info("Added revision to resource: {} revision: {}", resourceUrl, outputRevision);
  }

  @Override
  public void open(Configuration parameters) {
    revisionsClient = new RevisionServiceClient(
        properties.getProperty(MCS_URL),
        properties.getProperty(TOPOLOGY_USER_NAME),
        properties.getProperty(TOPOLOGY_USER_PASSWORD));
    LOGGER.info("Created operator adding revisions.");
  }

  private static Revision prepareRevisionEntity(WrittenRecordTuple tuple) {
    Revision outputRevision = new Revision();
    outputRevision.setRevisionProviderId(tuple.getTaskParams().getRevisionProviderId());
    outputRevision.setRevisionName(tuple.getTaskParams().getRevisionName());
    outputRevision.setCreationTimeStamp(tuple.getTaskParams().getRevisionCreationTimeStamp());
    return outputRevision;
  }

}
