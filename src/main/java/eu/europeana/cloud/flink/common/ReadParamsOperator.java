package eu.europeana.cloud.flink.common;

import static eu.europeana.cloud.flink.common.sink.CassandraClusterBuilder.getCassandraConnectionProvider;
import static eu.europeana.cloud.service.dps.PluginParameterKeys.NEW_REPRESENTATION_NAME;
import static eu.europeana.cloud.service.dps.PluginParameterKeys.OUTPUT_MIME_TYPE;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.flink.common.tuples.RecordParams;
import eu.europeana.cloud.flink.common.tuples.TaskParams;
import eu.europeana.cloud.flink.common.tuples.TaskRecordTuple;
import eu.europeana.cloud.service.commons.urls.DataSetUrlParser;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.DpsRecord;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Properties;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadParamsOperator extends RichMapFunction<DpsRecord, TaskRecordTuple> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReadParamsOperator.class);

  private final Properties properties;
  private transient CassandraTaskInfoDAO taskInfoDAO;

  public ReadParamsOperator(Properties properties) {
    this.properties = properties;
  }

  @Override
  public TaskRecordTuple map(DpsRecord dpsRecord) throws Exception {
    TaskRecordTuple resultTuple = TaskRecordTuple.builder()
                                                 .taskParams(createTaskParams(dpsRecord))
                                                 .recordParams(createRecordParams(dpsRecord)).
                                                 build();
    LOGGER.debug("Read parameters for tuple: {}", resultTuple);
    return resultTuple;
  }

  @NotNull
  private TaskParams createTaskParams(DpsRecord dpsRecord) throws IOException {
    TaskInfo taskInfo = taskInfoDAO.findById(dpsRecord.getTaskId()).orElseThrow();
    DpsTask dpsTask = DpsTask.fromTaskInfo(taskInfo);
    Revision outputRevision = dpsTask.getOutputRevision();
    return TaskParams.builder()
                     .id(dpsRecord.getTaskId())
                     .sentDate(taskInfo.getSentTimestamp())
                     .outputMimeType(dpsTask.getParameters().getOrDefault(OUTPUT_MIME_TYPE, "text/plain"))
                     .providerId(dpsTask.getParameter(PluginParameterKeys.PROVIDER_ID))
                     .dataSetId(extractDatasetId(dpsTask))
                     .newRepresentationName(dpsTask.getParameter(NEW_REPRESENTATION_NAME))
                     .revisionProviderId(outputRevision.getRevisionProviderId())
                     .revisionName(outputRevision.getRevisionName())
                     .revisionCreationTimeStamp(outputRevision.getCreationTimeStamp())
                     .build();
  }

  private RecordParams createRecordParams(DpsRecord dpsRecord) throws MalformedURLException {
    UrlParser urlParser = new UrlParser(dpsRecord.getRecordId());
    return RecordParams.builder()
                       .resourceUrl(dpsRecord.getRecordId())
                       .markedAsDeleted(dpsRecord.isMarkedAsDeleted())
                       .cloudId(urlParser.getPart(UrlPart.RECORDS))
                       .representationName(urlParser.getPart(UrlPart.REPRESENTATIONS))
                       .representationVersion(urlParser.getPart(UrlPart.VERSIONS))
                       .build();
  }

  @Override
  public void open(Configuration parameters) {
    CassandraConnectionProvider cassandraConnectionProvider = getCassandraConnectionProvider(properties);
    taskInfoDAO = new CassandraTaskInfoDAO(cassandraConnectionProvider);
    LOGGER.info("Created reading parameters operator.");
  }

  public static String extractDatasetId(DpsTask task) throws MalformedURLException {
    DataSet dataset = DataSetUrlParser.parse(task.getParameter(PluginParameterKeys.OUTPUT_DATA_SETS));
    return dataset.getId();
  }

}
