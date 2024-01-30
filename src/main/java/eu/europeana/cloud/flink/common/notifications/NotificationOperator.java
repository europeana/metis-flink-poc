package eu.europeana.cloud.flink.common.notifications;


import static eu.europeana.cloud.flink.common.sink.CassandraClusterBuilder.getCassandraConnectionProvider;

import com.datastax.driver.core.BatchStatement;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.dps.Notification;
import eu.europeana.cloud.flink.common.mcs.WriteRecordOperator;
import eu.europeana.cloud.flink.common.notifications.NotificationCounter.NotificationCounterBuilder;
import eu.europeana.cloud.flink.common.tuples.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.NotificationsDAO;
import java.util.Properties;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NotificationOperator extends RichMapFunction<NotificationTuple, NotificationTuple> {
  private static final Logger LOGGER = LoggerFactory.getLogger(NotificationOperator.class);
  private static final ReducingStateDescriptor<NotificationCounter> PROCESSED_RECORDS_STATE_DESCRIPTOR
      = new ReducingStateDescriptor<>("processedRecords", NotificationCounter::aggregate, NotificationCounter.class);

  private final Properties properties;
  private final String jobName;

  private transient ReducingState<NotificationCounter> processedRecords;

  private transient CassandraTaskInfoDAO taskInfoDAO;
  private transient NotificationsDAO subTaskInfoDAO;
  private transient CassandraConnectionProvider cassandraConnectionProvider;

  public NotificationOperator(String jobName, Properties properties) {
    this.jobName = jobName;
    this.properties = properties;
  }

  @Override
  public NotificationTuple map(NotificationTuple tuple) throws Exception {
    updateCounters(tuple);

    NotificationCounter counter = processedRecords.get();

    BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.LOGGED);
    batchStatement.add(subTaskInfoDAO.insertNotificationStatement(
        prepareNotification(tuple, counter.getProcessed())));
    batchStatement.add(taskInfoDAO.updateProcessedFilesStatement(
        tuple.getTaskId(),
        counter.getProcessedRecordsCount(),
        counter.getIgnoredRecordsCount(),
        counter.getDeletedRecordsCount(),
        counter.getProcessedErrorsCount(),
        counter.getDeletedErrorsCount()
    ));
    cassandraConnectionProvider.getSession().execute(batchStatement);

    LOGGER.info("Stored notifications and updated counters for: {}, counters: {}", tuple.getResource(), counter);
    return tuple;
  }

  private void updateCounters(NotificationTuple tuple) throws Exception {
    NotificationCounterBuilder counter = NotificationCounter.builder();
    if (tuple.isMarkedAsDeleted()) {
      counter.deletedRecordsCount(1);
    } else {
      counter.processedRecordsCount(1);
    }
    processedRecords.add(counter.build());
  }

  private Notification prepareNotification(NotificationTuple tuple, int resourceNum) {
    return Notification.builder().taskId(tuple.getTaskId())
                       .state("SUCCESS")
                       .infoText("")
                       .resource(tuple.getResource())
                       .resultResource(tuple.getResultResource())
                       .resourceNum(resourceNum)
                       .topologyName(jobName)
                       .build();
  }

  @Override
  public void open(Configuration parameters) {
    processedRecords = getRuntimeContext().getReducingState(PROCESSED_RECORDS_STATE_DESCRIPTOR);

    cassandraConnectionProvider = getCassandraConnectionProvider(properties);
    taskInfoDAO = new CassandraTaskInfoDAO(cassandraConnectionProvider);
    subTaskInfoDAO = new NotificationsDAO(cassandraConnectionProvider);

    LOGGER.info("Created operator storing notifications and progress.");
  }

}
