package eu.europeana.cloud.operator;

import eu.europeana.cloud.model.ExecutionRecord;
import eu.europeana.cloud.model.ExecutionRecordResult;
import eu.europeana.cloud.tool.JobName;
import eu.europeana.cloud.tool.JobParamName;
import eu.europeana.metis.transformation.service.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.nio.charset.StandardCharsets;

public class TransformationOperator extends ProcessFunction<ExecutionRecord, ExecutionRecordResult> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransformationOperator.class);

    private ParameterTool parameterTool;

    @Override
    public void open(Configuration parameters) throws Exception {
        parameterTool = ParameterTool.fromMap(getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap());
    }

    @Override
    public void processElement(ExecutionRecord sourceExecutionRecord, ProcessFunction<ExecutionRecord, ExecutionRecordResult>.Context ctx, Collector<ExecutionRecordResult> out) throws Exception {

        try {
            final XsltTransformer xsltTransformer = prepareXsltTransformer();

            StringWriter writer =
                    xsltTransformer.transform(
                            sourceExecutionRecord.getRecordData().getBytes(StandardCharsets.UTF_8),
                            prepareEuropeanaGeneratedIdsMap(sourceExecutionRecord));

            out.collect(
                    ExecutionRecordResult.from(
                            sourceExecutionRecord,
                            parameterTool.get(JobParamName.TASK_ID),
                            JobName.TRANSFORMATION,
                            writer.toString(),
                            null)
            );

            LOGGER.debug("Processed record, id: {}", sourceExecutionRecord.getExecutionRecordKey().getRecordId());
        } catch (Exception e) {
            LOGGER.warn("{} exception: {}", getClass().getName(), sourceExecutionRecord.getExecutionRecordKey().getRecordId(), e);
            out.collect(
                    ExecutionRecordResult.from(
                            sourceExecutionRecord,
                            parameterTool.get(JobParamName.TASK_ID),
                            JobName.TRANSFORMATION,
                            "",
                            e.getMessage())
            );
        }
    }

    private XsltTransformer prepareXsltTransformer()
            throws TransformationException {
        return new XsltTransformer(
                parameterTool.get(JobParamName.METIS_XSLT_URL),
                parameterTool.get(JobParamName.METIS_DATASET_NAME),
                parameterTool.get(JobParamName.METIS_DATASET_COUNTRY),
                parameterTool.get(JobParamName.METIS_DATASET_LANGUAGE));
    }

    private EuropeanaGeneratedIdsMap prepareEuropeanaGeneratedIdsMap(ExecutionRecord executionRecord)
            throws EuropeanaIdException {
        String metisDatasetId = parameterTool.get(JobParamName.DATASET_ID);
        //Prepare europeana identifiers
        EuropeanaGeneratedIdsMap europeanaGeneratedIdsMap = null;
        if (!StringUtils.isBlank(metisDatasetId)) {
            EuropeanaIdCreator europeanIdCreator = new EuropeanaIdCreator();
            europeanaGeneratedIdsMap = europeanIdCreator
                    .constructEuropeanaId(executionRecord.getRecordData(), metisDatasetId);
        }
        return europeanaGeneratedIdsMap;
    }
}
