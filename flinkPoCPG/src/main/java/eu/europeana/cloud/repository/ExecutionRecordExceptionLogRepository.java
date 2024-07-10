package eu.europeana.cloud.repository;

import eu.europeana.cloud.model.ExecutionRecordResult;
import eu.europeana.cloud.tool.DbConnectionProvider;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ExecutionRecordExceptionLogRepository implements DbRepository, Serializable {

    private final DbConnectionProvider dbConnectionProvider;

    public ExecutionRecordExceptionLogRepository(DbConnectionProvider dbConnectionProvider) {
        this.dbConnectionProvider = dbConnectionProvider;
    }

    public void save(ExecutionRecordResult executionRecordResult) {
        try (Connection con = dbConnectionProvider.getConnection();
             PreparedStatement preparedStatement = con.prepareStatement(
                     "INSERT INTO \"batch-framework\".execution_record_exception_log (DATASET_ID,EXECUTION_ID,EXECUTION_NAME, RECORD_ID, exception) VALUES (?,?,?,?,?)")
        ) {

            preparedStatement.setString(1, executionRecordResult.getExecutionRecord().getExecutionRecordKey().getDatasetId());
            preparedStatement.setString(2, executionRecordResult.getExecutionRecord().getExecutionRecordKey().getExecutionId());
            preparedStatement.setString(3, executionRecordResult.getExecutionRecord().getExecutionName());
            preparedStatement.setString(4, executionRecordResult.getExecutionRecord().getExecutionRecordKey().getRecordId());
            preparedStatement.setString(5, executionRecordResult.getException());
            preparedStatement.execute();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
