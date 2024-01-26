package org.flink.poc;

import eu.europeana.enrichment.rest.client.report.ProcessedResult;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/*
  Throttling
  Out of the box apache flink doesn't provide any way to throttle source.
  There is link to example throttled iterator what can be used as some kind workaround to limit rate of processing:
  https://github.com/apache/flink/blob/master/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/utils/ThrottledIterator.java
 */

/*
  Task cancellation
  There is only possibility to cancel whole Stream and jump directly to cancelling phase.
  That solution won't suffice for our needs since we need to cancel only parts involving certain task ids not whole stream that might
  include multiple task IDs. Similarly to Spark SS I implemented pseudo cache to store cancelled task IDs that are further used to
  filter out tasks that are to be cancelled.
 */

/*
  Task fairness
  Apache Flink works hard to auto-derive sensible default resource requirements for all applications out of the box.
  There is way to manually fine tune resource allocated via fine-grained resource functionality.
  Read more: https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/finegrained_resource/
  It would allow us to make jobs for given stream consume less resources. It would require us having possibly multiple streams for
  different type of task loads. It is workaround, and it would require more research and a lot of planning to implement properly.
 */

/*
  Task progress
  Apache flink provide with monitoring tool that let us see metrics such as uptime of job, number of restarts,
   number of completed checkpoints and number of failed checkpoints. Again those metrics are not really helpful beside checking health of cluster.
   Similarly to Spark SS,distributed store was used to preserve counters that can act as task progress indicators.
   Store in flink is way less limited than in spark SS what make it way easier to work with and gives higher flexibility in terms of
   what can be achieved via using it.
 */

/*
  Fault Tolerance
  Given that Flink recovers from faults by rewinding and replaying the source data streams (require idempotent or retryable streams),
   when the ideal situation is described as exactly once this does not mean that every event will be processed exactly once.
   It means that every event will affect the state being managed by Flink exactly once. This mechanism similarly as spark SS is based
   on snapshotting.
 */

/*
  My thoughts
  I feel like Flink is better to work with than Spark SS. It isn't bound by same restrictions as Spark SS and gives higher flexibility.
  Additionally, it gives better debugging feedback what gives better experience working with that tool. I feel like It is tool worth further research.
 */
public class FlinkPoC {


  private static final Logger LOGGER = LoggerFactory.getLogger(FlinkPoC.class);
  private static final Properties enrichmentProperties = new Properties();
  private static Enricher enricher;
  private static StreamExecutionEnvironment flinkEnvironment;

  private static final Set<String> canceledTasks = new HashSet<>();

  public static void main(String[] args) throws Exception {
    init(args);

    DataStream<String> inputStream = initiateSocketStream(9999);


    DataStream<TaskData> taskData = prepareTaskDataForProcessing(inputStream);

    DataStream<TaskData> enrichedTd = enrichTaskData(taskData);

    prepareStatistics(enrichedTd);

    enrichedTd.print();

    DataStream<String> cancelledTasksInputStream = initiateSocketStream(9998);

    cancelledTasksInputStream.flatMap(new ReadCanceledIds()).print();

    flinkEnvironment.execute("Windowed word count");

  }

  private static DataStreamSink<TaskStatistics> prepareStatistics(DataStream<TaskData> enrichedTd) {
    return enrichedTd
        .keyBy(new KeySelector<TaskData, String>() {
          @Override
          public String getKey(TaskData taskData) throws Exception {
            return taskData.getTaskId();
          }
        })
        .map(new StatisticsMap())
        .filter(td -> !canceledTasks.contains(td.getTaskId()))
        .print();
  }

  private static SingleOutputStreamOperator<TaskData> enrichTaskData(DataStream<TaskData> taskData) {
    return taskData
                    .filter(td -> !canceledTasks.contains(td.getTaskId()))
                    .map((MapFunction<TaskData, TaskData>) td -> {
                     td.setFileContent(new FileManager().readFileData(td.getFileUrl()));
                     return td;
                   })
                   .map((MapFunction<TaskData, TaskData>) td -> {
                     ProcessedResult<String> enrichmentResult = enricher.enrich(td.getFileContent());
                     td.setReportSet(enrichmentResult.getReport());
                     td.setProcessingStatus(enrichmentResult.getRecordStatus().toString());
                     td.setResultFileContent(enrichmentResult.getProcessedRecord());
                     return td;
                   })
                    .filter(td -> !canceledTasks.contains(td.getTaskId()));
  }

  private static SingleOutputStreamOperator<TaskData> prepareTaskDataForProcessing(DataStream<String> inputStream) {
    return inputStream
        .map((MapFunction<String, String[]>) td -> td.split(" "))
        .filter((FilterFunction<String[]>) td -> td.length == 3)
        .map((MapFunction<String[], TaskData>) td -> new TaskData(td[0], td[1], td[2]));
  }

  private static void enrichWorkflow() {

  }

  private static DataStream<String> initiateSocketStream(int port) {
    return flinkEnvironment.socketTextStream("localhost", port);
  }

  private static void init(String[] args) {
    if (args.length != 1) {
      throw new RuntimeException(
          "Please provide program with proper parameters!\n"
              + "First parameter is location of file containing EnrichmentProperties (For example enrichment_topology_config.properties)\n");
    }
    try (FileInputStream fileInput = new FileInputStream(args[0])) {
      LOGGER.info("Config file provided!");
      enrichmentProperties.load(fileInput);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    enricher = new Enricher(
        enrichmentProperties.getProperty("DEREFERENCE_SERVICE_URL"),
        enrichmentProperties.getProperty("ENTITY_MANAGEMENT_URL"),
        enrichmentProperties.getProperty("ENTITY_API_URL"),
        enrichmentProperties.getProperty("ENTITY_API_KEY")
    );

    flinkEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
  }

  public static class ReadCanceledIds implements FlatMapFunction<String, String> {

    @Override
    public void flatMap(String inputLines, Collector<String> collector) throws Exception {
        Arrays.stream(inputLines.split(" ")).forEach(id -> {
          canceledTasks.add(id);
          collector.collect(id);
    });
  }
  }
  public static class StatisticsMap extends RichMapFunction<TaskData, TaskStatistics> {

    ValueState<TaskStatistics> taskStatistics;

    StateTtlConfig ttlConfig = StateTtlConfig
        .newBuilder(Time.minutes(5))
        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
        .build();


    @Override
    public void open(Configuration configuration) {
      ValueStateDescriptor<TaskStatistics> desc = new ValueStateDescriptor<>("taskStatistics", Types.POJO(TaskStatistics.class));
      desc.enableTimeToLive(ttlConfig);
      taskStatistics = getRuntimeContext().getState(desc);
    }

    @Override
    public TaskStatistics map(TaskData taskData) throws Exception {
      if (taskStatistics.value() == null) {
        taskStatistics.update(new TaskStatistics(taskData.getTaskId()));
      }
      TaskStatistics ts = taskStatistics.value();
      if ("STOP".equals(taskData.getProcessingStatus())) {
        ts.failedRecordProcessed += 1;
      } else {
        ts.succeededRecordProcessed += 1;
      }
      taskStatistics.update(ts);
      return ts;
    }
  }

}
