package eu.europeana.cloud.flink.oai;


import eu.europeana.cloud.flink.oai.source.OAISource;
import eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys;
import eu.europeana.metis.harvesting.HarvesterException;
import eu.europeana.metis.harvesting.oaipmh.OaiRecordHeader;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OAIJob {

  private static final Logger LOGGER = LoggerFactory.getLogger(OAIJob.class);
  protected static final Properties properties = new Properties();

  protected final StreamExecutionEnvironment flinkEnvironment;
  protected final String jobName;
  
  protected final DataStreamSource<OaiRecordHeader> source;
 
  
  public OAIJob(String propertyPath) throws Exception {
    try (FileInputStream fileInput = new FileInputStream(propertyPath)) {
      properties.load(fileInput);
    }
    jobName = properties.getProperty(TopologyPropertyKeys.TOPOLOGY_NAME);
    flinkEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

    source = flinkEnvironment.fromSource(
        new OAISource(), WatermarkStrategy.noWatermarks(), "OAI Source").setParallelism(1);
    source.map(new HeaderPrintOperator());
  }


  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      throw new RuntimeException("exactly one parameter, containing configuration property file path is needed!");
    }
    OAIJob job = new OAIJob(args[0]);
    job.execute();
  }

  private void execute() throws Exception {
    flinkEnvironment.execute();
  }

}
