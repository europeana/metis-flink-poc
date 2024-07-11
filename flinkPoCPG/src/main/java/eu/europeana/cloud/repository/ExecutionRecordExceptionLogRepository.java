package eu.europeana.cloud.repository;

import eu.europeana.cloud.model.ExecutionRecordResult;
import eu.europeana.cloud.tool.DbConnectionProvider;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ExecutionRecordExceptionLogRepository implements DbRepository, Serializable {

    private static final String NO_OF_ELEMENTS = "select count(*) as elements from \"batch-framework\".execution_record_exception_log where dataset_id = ? and execution_id = ?";

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

    public long countByDatasetIdAndExecutionId(String datasetId, String executionId) throws IOException {

        ResultSet resultSet;
        try (PreparedStatement preparedStatement = dbConnectionProvider.getConnection().prepareStatement(NO_OF_ELEMENTS)) {
            preparedStatement.setString(1, datasetId);
            preparedStatement.setString(2, executionId);

            resultSet = preparedStatement.executeQuery();

            if (resultSet.next()) {
                return resultSet.getLong("elements");
            } else {
                return 0L;
            }
        } catch(SQLException e){
            throw new IOException(e);
        }
    }

}
