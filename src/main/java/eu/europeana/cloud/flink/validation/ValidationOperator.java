package eu.europeana.cloud.flink.validation;

import eu.europeana.cloud.flink.common.tuples.ErrorTuple;
import eu.europeana.cloud.flink.common.tuples.RecordTuple;
import eu.europeana.cloud.service.dps.storm.topologies.properties.PropertyFileLoader;
import eu.europeana.cloud.service.dps.storm.topologies.validation.topology.ValidationTopologyPropertiesKeys;
import eu.europeana.metis.transformation.service.TransformationException;
import eu.europeana.metis.transformation.service.XsltTransformer;
import eu.europeana.validation.model.ValidationResult;
import eu.europeana.validation.service.ValidationExecutionService;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ValidationOperator extends ProcessFunction<RecordTuple, RecordTuple> {

  public static final OutputTag<ErrorTuple> ERROR_STREAM_TAG = new OutputTag<>("error-stream") {
  };
  private static final String VALIDATION_PROPERTIES_FILE = "validation.properties";
  private static final Logger LOGGER = LoggerFactory.getLogger(ValidationOperator.class);
  private final ValidationTaskParams taskParams;
  private transient XsltTransformer transformer;
  private transient ValidationExecutionService validationService;

  public ValidationOperator(ValidationTaskParams taskParams) {
    this.taskParams = taskParams;
  }

  @Override
  public void processElement(RecordTuple tuple, ProcessFunction<RecordTuple, RecordTuple>.Context context,
      Collector<RecordTuple> out) {
    try {
      byte[] sortedContent = reorderFileContent(tuple.getFileContent());
      validate(sortedContent);
      LOGGER.info("Validated record: {}", tuple.getRecordId());
      out.collect(tuple);
    } catch (Exception e) {
      LOGGER.warn("Validating record error: {}", tuple.getRecordId(), e);
      context.output(ERROR_STREAM_TAG, ErrorTuple.builder()
                                                 .recordId(tuple.getRecordId())
                                                 .exception(e)
                                                 .build());
    }
  }


  private void validate(byte[] sortedContent) {
    String document = new String(sortedContent, StandardCharsets.UTF_8);
    ValidationResult result = validationService.singleValidation(taskParams.getSchemaName(),
        taskParams.getRootLocation(), taskParams.getSchematronLocation(), document);

    if (!result.isSuccess()) {
      throw new RuntimeException("Validation failed! " + result.getMessage());
    }

  }

  private byte[] reorderFileContent(byte[] fileContent) throws TransformationException {
    StringWriter writer = transformer.transform(fileContent, null);
    return writer.toString().getBytes(StandardCharsets.UTF_8);
  }

  public void open(Configuration parameters) throws TransformationException {
    Properties validationProperties = new Properties();
    PropertyFileLoader.loadPropertyFile(VALIDATION_PROPERTIES_FILE, "", validationProperties);
    validationService = new ValidationExecutionService(validationProperties);
    String sorterFileLocation = validationProperties.get(ValidationTopologyPropertiesKeys.EDM_SORTER_FILE_LOCATION).toString();
    LOGGER.info("Preparing XsltTransformer for {}", sorterFileLocation);
    transformer = new XsltTransformer(sorterFileLocation);
    LOGGER.info("Created validation operator.");
  }
}
