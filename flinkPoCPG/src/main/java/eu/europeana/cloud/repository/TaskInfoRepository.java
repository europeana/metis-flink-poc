package eu.europeana.cloud.repository;

import eu.europeana.cloud.tool.DbConnection;
import eu.europeana.cloud.model.TaskInfo;
import eu.europeana.cloud.exception.TaskInfoNotFoundException;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TaskInfoRepository implements DbRepository, Serializable {

    private final DbConnection dbConnection;

    public TaskInfoRepository(DbConnection dbConnection) {
        this.dbConnection = dbConnection;
    }

    public void save(TaskInfo taskInfo) {
        try (PreparedStatement preparedStatement = dbConnection.getConnection().prepareStatement(
                "INSERT INTO \"batch-framework\".task_info (TASK_ID,COMMIT_COUNT,WRITE_COUNT) VALUES (?,?,?)")) {
            preparedStatement.setLong(1, taskInfo.taskId());
            preparedStatement.setLong(2, taskInfo.commitCount());
            preparedStatement.setLong(3, taskInfo.writeCount());

            preparedStatement.execute();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    public void update(TaskInfo taskInfo) {
        try (PreparedStatement preparedStatement = dbConnection.getConnection().prepareStatement(
                "update \"batch-framework\".task_info SET commit_count=?, write_count=? where task_id = ?")) {
            preparedStatement.setLong(1, taskInfo.commitCount());
            preparedStatement.setLong(2, taskInfo.writeCount());
            preparedStatement.setLong(3, taskInfo.taskId());

            preparedStatement.execute();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }



    public TaskInfo get(long taskId) throws TaskInfoNotFoundException {
        try (PreparedStatement preparedStatement = dbConnection.getConnection().prepareStatement(
                "SELECT * FROM \"batch-framework\".task_info WHERE TASK_ID = ?")) {
            preparedStatement.setLong(1, taskId);


            ResultSet resultSet = preparedStatement.executeQuery();

            if (resultSet.next()) {
                return new TaskInfo(
                        resultSet.getLong("TASK_ID"),
                        resultSet.getLong("COMMIT_COUNT"),
                        resultSet.getLong("WRITE_COUNT")

                );
            } else {
                throw new TaskInfoNotFoundException();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        dbConnection.close();
    }
}
