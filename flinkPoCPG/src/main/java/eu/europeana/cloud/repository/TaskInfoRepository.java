package eu.europeana.cloud.repository;

import eu.europeana.cloud.exception.TaskInfoNotFoundException;
import eu.europeana.cloud.model.TaskInfo;
import eu.europeana.cloud.tool.DbConnectionProvider;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TaskInfoRepository implements DbRepository, Serializable {

    private final DbConnectionProvider dbConnectionProvider;

    public TaskInfoRepository(DbConnectionProvider dbConnectionProvider) {
        this.dbConnectionProvider = dbConnectionProvider;
    }

    public void save(TaskInfo taskInfo) {
        try (Connection con = dbConnectionProvider.getConnection();
             PreparedStatement preparedStatement = con.prepareStatement(
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
        try (Connection con = dbConnectionProvider.getConnection();
             PreparedStatement preparedStatement = con.prepareStatement(
                     "update \"batch-framework\".task_info SET commit_count=?, write_count=? where task_id = ?")) {
            preparedStatement.setLong(1, taskInfo.commitCount());
            preparedStatement.setLong(2, taskInfo.writeCount());
            preparedStatement.setLong(3, taskInfo.taskId());

            preparedStatement.execute();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void incrementWriteCount(long taskId, long writeCountIncrement) {
        try (Connection con = dbConnectionProvider.getConnection();
             /*
             Be aware that approach only works in database supporting atomic updated such as postgresql
              */
             PreparedStatement preparedStatement = con.prepareStatement(
                     "update \"batch-framework\".task_info SET commit_count=commit_count+1, write_count=write_count+? where task_id = ?")) {
            preparedStatement.setLong(1, writeCountIncrement);
            preparedStatement.setLong(2, taskId);

            preparedStatement.execute();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public TaskInfo get(long taskId) throws TaskInfoNotFoundException {
        try (Connection con = dbConnectionProvider.getConnection();
             PreparedStatement preparedStatement = con.prepareStatement(
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
                throw new TaskInfoNotFoundException(taskId);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
