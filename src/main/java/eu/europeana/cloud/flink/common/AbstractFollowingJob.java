package eu.europeana.cloud.flink.common;

import static eu.europeana.cloud.flink.simpledb.SimpleDbCassandraSourceBuilder.createCassandraSource;
import static eu.europeana.cloud.flink.common.FollowingJobMainOperator.ERROR_STREAM_TAG;

import eu.europeana.cloud.flink.common.sink.CassandraClusterBuilder;
import eu.europeana.cloud.flink.common.tuples.RecordTuple;
import eu.europeana.cloud.flink.simpledb.DbEntityCreatingOperator;
import eu.europeana.cloud.flink.simpledb.DbEntityToTupleConvertingOperator;
import eu.europeana.cloud.flink.simpledb.DbErrorEntityCreatingOperator;
import eu.europeana.cloud.flink.simpledb.RecordExecutionEntity;
import eu.europeana.cloud.flink.simpledb.RecordExecutionExceptionLogEntity;
import eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys;
import java.util.Properties;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractFollowingJob<PARAMS_TYPE extends FollowingTaskParams> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractFollowingJob.class);

  protected final StreamExecutionEnvironment flinkEnvironment;

  protected AbstractFollowingJob(Properties properties, PARAMS_TYPE taskParams) throws Exception {
    LOGGER.info("Creating {} for execution: {}, with execution parameters: {}",
        getClass().getSimpleName(), taskParams.getExecutionId(), taskParams);
    String jobName = properties.getProperty(TopologyPropertyKeys.TOPOLOGY_NAME);
    flinkEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStreamSource<RecordExecutionEntity> source = createCassandraSource(flinkEnvironment, properties, taskParams)
        //This ensure rebalancing tuples emitted by this source, so they are performed in parallel on next steps
        //TODO The command rebalance does not work for this source for some reasons. To investigate
        .setParallelism(1);


    SingleOutputStreamOperator<RecordTuple> processStream =
        source.map(new DbEntityToTupleConvertingOperator())
              .process(createMainOperator(properties, taskParams));

    SingleOutputStreamOperator<RecordExecutionEntity> resultStream =
        processStream.map(new DbEntityCreatingOperator(jobName, taskParams));

    SingleOutputStreamOperator<RecordExecutionExceptionLogEntity> errorStream =
        processStream.getSideOutput(ERROR_STREAM_TAG)
                     .map(new DbErrorEntityCreatingOperator(jobName, taskParams));

    CassandraClusterBuilder cassandraClusterBuilder = new CassandraClusterBuilder(properties);
    CassandraSink.addSink(resultStream)
                 .setClusterBuilder(cassandraClusterBuilder)
                 .build();
    CassandraSink.addSink(errorStream)
                 .setClusterBuilder(cassandraClusterBuilder)
                 .build();

  }

  protected abstract FollowingJobMainOperator createMainOperator(Properties properties, PARAMS_TYPE taskParams);

  protected void execute() throws Exception {
    flinkEnvironment.execute();
  }

}
