package eu.europeana.cloud.flink.normalization;

import eu.europeana.cloud.flink.common.RetrieveFileOperator;
import eu.europeana.cloud.flink.common.AbstractJob;
import eu.europeana.cloud.flink.common.tuples.FileTuple;
import eu.europeana.cloud.flink.common.FileTuplePrintOperator;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public class NormalizationJob extends AbstractJob {

  public NormalizationJob(String propertyPath) throws Exception {
    super(propertyPath);

    SingleOutputStreamOperator<FileTuple> resultStream = source.map(new RetrieveFileOperator(properties))
                                                               .map(new FileTuplePrintOperator("BEFORE NORMALIZATION"))
                                                               .map(new NormalizationOperator())
                                                               .map(new FileTuplePrintOperator("AFTER NORMALIZATION"));
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.out.println("exactly one parameter, containing configuration property file path needed!");
    }
    NormalizationJob topoplogy = new NormalizationJob(args[0]);
    topoplogy.execute();
  }
}
