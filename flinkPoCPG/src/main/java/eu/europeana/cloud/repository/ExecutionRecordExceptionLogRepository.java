package eu.europeana.cloud.repository;

import eu.europeana.cloud.model.ExecutionRecordResult;
import eu.europeana.cloud.tool.DbConnection;

import java.io.IOException;
import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ExecutionRecordExceptionLogRepository implements DbRepository, Serializable {

    private final DbConnection dbConnection;

    public ExecutionRecordExceptionLogRepository(DbConnection dbConnection) {
        this.dbConnection = dbConnection;
    }

    public void save(ExecutionRecordResult executionRecordResult) {
        try {
            PreparedStatement preparedStatement = dbConnection.getConnection().prepareStatement(
                    "INSERT INTO \"batch-framework\".execution_record_exception_log (DATASET_ID,EXECUTION_ID,EXECUTION_NAME, RECORD_ID, exception) VALUES (?,?,?,?,?)");

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

    @Override
    public void close() throws IOException {
        dbConnection.close();
    }
}
