package eu.europeana.cloud.operator;

import eu.europeana.cloud.model.ExecutionRecord;
import eu.europeana.cloud.model.ExecutionRecordResult;
import eu.europeana.cloud.tool.JobName;
import eu.europeana.cloud.tool.JobParamName;
import eu.europeana.normalization.Normalizer;
import eu.europeana.normalization.NormalizerFactory;
import eu.europeana.normalization.model.NormalizationResult;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NormalizationOperator extends ProcessFunction<ExecutionRecord, ExecutionRecordResult> {

    private static final Logger LOGGER = LoggerFactory.getLogger(NormalizationOperator.class);

    private transient NormalizerFactory normalizerFactory;
    private ParameterTool parameterTool;

    @Override
    public void open(Configuration parameters) throws Exception {
        normalizerFactory = new NormalizerFactory();
        parameterTool = ParameterTool.fromMap(getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap());
        LOGGER.info("Created normalization operator.");
    }


    @Override
    public void processElement(ExecutionRecord sourceExecutionRecord, ProcessFunction<ExecutionRecord, ExecutionRecordResult>.Context ctx, Collector<ExecutionRecordResult> out) throws Exception {

        final Normalizer normalizer = normalizerFactory.getNormalizer();

        NormalizationResult normalizationResult = normalizer.normalize(sourceExecutionRecord.getRecordData());
        if (normalizationResult.getErrorMessage() == null) {
            out.collect(
                    ExecutionRecordResult.from(
                            sourceExecutionRecord,
                            parameterTool.get(JobParamName.TASK_ID),
                            JobName.NORMALIZATION,
                            normalizationResult.getNormalizedRecordInEdmXml(),
                            null)
            );
        } else {
            out.collect(
                    ExecutionRecordResult.from(
                            sourceExecutionRecord,
                            parameterTool.get(JobParamName.TASK_ID),
                            JobName.NORMALIZATION,
                            "",
                            normalizationResult.getErrorMessage())
            );
        }
    }
}
