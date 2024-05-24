package eu.europeana.cloud.flink.validation;

import eu.europeana.cloud.flink.common.FollowingJobMainOperator;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ValidationOperator extends FollowingJobMainOperator {

  private static final String VALIDATION_PROPERTIES_FILE = "validation.properties";
  private static final Logger LOGGER = LoggerFactory.getLogger(ValidationOperator.class);
  private final ValidationTaskParams taskParams;
  private transient XsltTransformer transformer;
  private transient ValidationExecutionService validationService;

  public ValidationOperator(ValidationTaskParams taskParams) {
    this.taskParams = taskParams;
  }

  @Override
  public RecordTuple map(RecordTuple tuple) throws Exception {
    try {
      validate(getSortedFileContent(tuple.getFileContent()));
      return tuple;
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      return RecordTuple.builder()
                        .recordId(tuple.getRecordId())
                        .fileContent(tuple.getFileContent())
                        .errorMessage(e.getMessage())
                        .build();
    }
  }

  @Override
  public void open(Configuration parameters) {
    try {
      Properties validationProperties = new Properties();
      PropertyFileLoader.loadPropertyFile(VALIDATION_PROPERTIES_FILE, "", validationProperties);
      validationService = new ValidationExecutionService(validationProperties);
      final String sorterFileLocation = validationProperties.get(ValidationTopologyPropertiesKeys.EDM_SORTER_FILE_LOCATION)
                                                            .toString();
      LOGGER.info("Preparing XsltTransformer for {}", sorterFileLocation);
      transformer = new XsltTransformer(sorterFileLocation);
      LOGGER.info("Created validation operator.");
    } catch (Exception e) {
      LOGGER.warn("Validation service not available {}", e.getMessage(), e);
    }
  }

  @Override
  public void close() throws Exception {
    validationService.cleanup();
    //We do not close XsltTransformer, cause its close method closes static resource, so looks to be improper.
    // The same is in the XsltBolt in the eCloud code.
    // transformer.close();
  }

  private void validate(byte[] sortedContent) {
    String document = new String(sortedContent, StandardCharsets.UTF_8);
    ValidationResult result = validationService.singleValidation(taskParams.getSchemaName(),
        taskParams.getRootLocation(), taskParams.getSchematronLocation(), document);

    if (!result.isSuccess()) {
      throw new RuntimeException("Validation failed! " + result.getMessage());
    }

  }

  private byte[] getSortedFileContent(byte[] fileContent) throws TransformationException {
    StringWriter writer = transformer.transform(fileContent, null);
    return writer.toString().getBytes(StandardCharsets.UTF_8);
  }

}
