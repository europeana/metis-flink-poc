package eu.europeana.cloud.repository;

import eu.europeana.cloud.model.ExecutionRecordResult;
import eu.europeana.cloud.tool.DbConnection;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ExecutionRecordRepository implements DbRepository, Serializable {

    private final DbConnection dbConnection;

    private static final String NO_OF_ELEMENTS = "select count(*) as elements from \"batch-framework\".execution_record where dataset_id = ? and execution_id = ?";
    private static final String LIMIT = "select * from \"batch-framework\".execution_record where dataset_id = ? and execution_id = ? offset ? limit ?;";

    public ExecutionRecordRepository(DbConnection dbConnection) {

        this.dbConnection = dbConnection;
    }

    public void save(ExecutionRecordResult executionRecordResult) {
        try {
            PreparedStatement preparedStatement = dbConnection.getConnection().prepareStatement(
                    "INSERT INTO \"batch-framework\".execution_record (DATASET_ID,EXECUTION_ID,EXECUTION_NAME, RECORD_ID, RECORD_DATA) VALUES (?,?,?,?,?)");

            preparedStatement.setString(1, executionRecordResult.getExecutionRecord().getExecutionRecordKey().getDatasetId());
            preparedStatement.setString(2, executionRecordResult.getExecutionRecord().getExecutionRecordKey().getExecutionId());
            preparedStatement.setString(3, executionRecordResult.getExecutionRecord().getExecutionName());
            preparedStatement.setString(4, executionRecordResult.getExecutionRecord().getExecutionRecordKey().getRecordId());
            preparedStatement.setString(5, new String(executionRecordResult.getExecutionRecord().getRecordData()));
            preparedStatement.execute();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public long countByDatasetIdAndExecutionId(String datasetId, String executionId) throws SQLException {

        ResultSet resultSet;
        try (PreparedStatement preparedStatement = dbConnection.getConnection().prepareStatement(NO_OF_ELEMENTS)) {
            preparedStatement.setString(1, datasetId);
            preparedStatement.setString(2, executionId);

            resultSet = preparedStatement.executeQuery();

            if (resultSet.next()) {
                return resultSet.getLong("elements");
            } else {
                return 0L;
            }
        }
    }

    public ResultSet getByDatasetIdAndExecutionIdAndOffsetAndLimit(String datasetId, String executionId, long offset, long limit) throws SQLException {
        PreparedStatement preparedStatement = dbConnection.getConnection().prepareStatement(LIMIT);
        preparedStatement.setString(1, datasetId);
        preparedStatement.setString(2, executionId);
        preparedStatement.setLong(3, offset);
        preparedStatement.setLong(4, limit);

        return preparedStatement.executeQuery();
    }

    @Override
    public void close() {
        dbConnection.close();
    }
}
