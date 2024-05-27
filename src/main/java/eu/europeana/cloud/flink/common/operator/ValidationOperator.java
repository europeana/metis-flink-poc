package eu.europeana.cloud.flink.common.operator;

import eu.europeana.cloud.flink.model.ExecutionRecord;
import eu.europeana.metis.transformation.service.TransformationException;
import eu.europeana.metis.transformation.service.XsltTransformer;
import eu.europeana.validation.model.ValidationResult;
import eu.europeana.validation.service.ValidationExecutionService;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.util.Properties;
import java.util.Random;

public class ValidationOperator extends ProcessFunction<ExecutionRecord, ExecutionRecord> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ValidationOperator.class);
    private static final String EDM_SORTER_FILE_URL = "http://ftp.eanadev.org/schema_zips/edm_sorter_20230809.xsl";
    private transient XsltTransformer transformer;
    private transient ValidationExecutionService validationService;
    private long taskId;
    private String schema;
    private String rootFileLocation;
    private String schematronFileLocation;

    @Override
    public void open(Configuration parameters) throws Exception {
        LOGGER.info("Opening validation operator");
        ParameterTool parameterTool = ParameterTool.fromMap(getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap());
        taskId = parameterTool.getLong("TASK_ID");
        //
        Properties validationProperties = prepareProperties();
        validationService = new ValidationExecutionService(validationProperties);
        transformer = new XsltTransformer(EDM_SORTER_FILE_URL);
        //
        schema = validationProperties.getProperty("predefinedSchemas.edm-external.url");
        rootFileLocation = validationProperties.getProperty("predefinedSchemas.edm-external.rootLocation");
        schematronFileLocation = validationProperties.getProperty("predefinedSchemas.edm-external.schematronLocation");
    }

    @Override
    public void processElement(ExecutionRecord value, ProcessFunction<ExecutionRecord, ExecutionRecord>.Context ctx, Collector<ExecutionRecord> out) throws Exception {
        LOGGER.info("Validating record with id {} on instance: {}", value.getExecutionRecordKey().getRecordId(), this);

        /*This is done by purpose to artificially break the ordering of processed records*/
        Thread.sleep(new Random().nextLong(10000));
        //
        reorderFileContent(value);
        validateFileAndEmit(value);
        //
        value.getExecutionRecordKey().setExecutionId(taskId + "");
        out.collect(value);
    }

    private void reorderFileContent(ExecutionRecord executionRecord) throws TransformationException {
        LOGGER.info("Reordering the file");
        StringWriter writer = transformer.transform(executionRecord.getRecordData().getBytes(), null);
        executionRecord.setRecordData(writer.toString());
    }

    private void validateFileAndEmit(ExecutionRecord executionRecord) {
        String document = executionRecord.getRecordData();
        ValidationResult result =
                validationService.singleValidation(
                        schema,
                        rootFileLocation,
                        schematronFileLocation,
                        document
                );
        if (result.isSuccess()) {
            LOGGER.debug("Validation Success for datasetId {}, recordId {}", executionRecord.getExecutionRecordKey().getDatasetId(),
                    executionRecord.getExecutionRecordKey().getRecordId());
        } else {
            LOGGER.info("Validation Failure for datasetId {}, recordId {}", executionRecord.getExecutionRecordKey().getDatasetId(),
                    executionRecord.getExecutionRecordKey().getRecordId());
            executionRecord.setRecordData(new String());
        }
    }

    private Properties prepareProperties() {
        Properties properties = new Properties();

        properties.setProperty("predefinedSchemas", "localhost");

        properties.setProperty("predefinedSchemas.edm-internal.url",
                "http://ftp.eanadev.org/schema_zips/europeana_schemas-20220809.zip");
        properties.setProperty("predefinedSchemas.edm-internal.rootLocation", "EDM-INTERNAL.xsd");
        properties.setProperty("predefinedSchemas.edm-internal.schematronLocation", "schematron/schematron-internal.xsl");

        properties.setProperty("predefinedSchemas.edm-external.url",
                "http://ftp.eanadev.org/schema_zips/europeana_schemas-20220809.zip");
        properties.setProperty("predefinedSchemas.edm-external.rootLocation", "EDM.xsd");
        properties.setProperty("predefinedSchemas.edm-external.schematronLocation", "schematron/schematron.xsl");

        return properties;

    }
}
