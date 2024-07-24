package eu.europeana.cloud.repository;

import eu.europeana.cloud.flink.client.constants.postgres.JobParamName;
import eu.europeana.cloud.model.ExecutionRecord;
import eu.europeana.cloud.model.ExecutionRecordKey;
import eu.europeana.cloud.model.ExecutionRecordResult;
import eu.europeana.cloud.tool.DbConnectionProvider;
import java.io.IOException;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class RepositoriesIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(RepositoriesIT.class);

  private ParameterTool parameterTool = ParameterTool.fromMap(Map.of(
      JobParamName.DATASOURCE_URL, "jdbc:postgresql://localhost:5432/flink_poc",
      JobParamName.DATASOURCE_USERNAME, "admin",
      JobParamName.DATASOURCE_PASSWORD, "admin")
  );
  private final DbConnectionProvider dbConnectionProvider = new DbConnectionProvider(parameterTool);
  private ExecutionRecordRepository recordRepository = new ExecutionRecordRepository(dbConnectionProvider);
  private ExecutionRecordExceptionLogRepository errorRepository = new ExecutionRecordExceptionLogRepository(dbConnectionProvider);

  @Test
  void shouldProperlyLogIfTheRecordAlreadyExistedInTheDb() throws IOException {
    ExecutionRecordResult result = ExecutionRecordResult.from(
        ExecutionRecord
            .builder()
            .recordData("<xml/>")
            .executionName("TEST_STEP")
            .executionRecordKey(
                ExecutionRecordKey
                    .builder()
                    .datasetId("JUnit")
                    .executionId(this.getClass().getSimpleName())
                    .recordId("/testing/record" + RandomStringUtils.randomAlphanumeric(7))
                    .build()
            ).build()
    );
    LOGGER.info("Storing the record first time");
    recordRepository.save(result);
    LOGGER.info("Storing try completed!");
    LOGGER.info("Storing the record second time");
    recordRepository.save(result);
    LOGGER.info("Storing try completed");
  }

  @Test
  void shouldProperlyLogIfTheErrorLogAlreadyExistedInTheDb() {
    ExecutionRecordResult result = ExecutionRecordResult
        .builder()
        .executionRecord(
            ExecutionRecord
                .builder()
                .executionName("TEST_STEP")
                .executionRecordKey(
                    ExecutionRecordKey
                        .builder()
                        .datasetId("JUnit")
                        .executionId(this.getClass().getSimpleName())
                        .recordId("/testing/errorLog"
                            + RandomStringUtils.randomAlphanumeric(7))
                        .build()
                ).build()
        )
        .exception("java.lang.NoSuchMethodError: 'org.exceptionhouse.noMethod")
        .build();
    LOGGER.info("Storing the error log first time");
    errorRepository.save(result);
    LOGGER.info("Storing try completed!");
    LOGGER.info("Storing the error log second time");
    errorRepository.save(result);
    LOGGER.info("Storing try completed");
  }


}