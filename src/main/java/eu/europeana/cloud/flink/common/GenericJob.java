package eu.europeana.cloud.flink.common;

import static eu.europeana.cloud.flink.common.JobsParametersConstants.CONFIGURATION_FILE_PATH;
import static eu.europeana.cloud.flink.common.utils.JobUtils.readProperties;

import java.io.IOException;
import java.util.Properties;
import org.apache.flink.api.common.JobExecutionResult;

public abstract class GenericJob<T> {

  protected abstract JobParameters<T> prepareParameters(String[] args) throws IOException;

  protected abstract void setupJob(Properties properties, T parameters) throws Exception;

  protected abstract JobExecutionResult execute() throws Exception;

  public JobExecutionResult executeJob(String[] args) throws Exception {
    JobParameters<T> parameters = prepareParameters(args);
    setupJob(readProperties(parameters.getTool().getRequired(CONFIGURATION_FILE_PATH)), parameters.getTaskParameters());
    return execute();
  }
}
