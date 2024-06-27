package eu.europeana.cloud.tool;


import org.apache.flink.api.java.utils.ParameterTool;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DbConnection implements Serializable {

    private transient Connection connection;
    private final String url;
    private final String user;
    private final String password;


    public DbConnection(ParameterTool parameterTool) {
        this.url = parameterTool.getRequired(JobParamName.DATASOURCE_URL);
        this.user = parameterTool.get(JobParamName.DATASOURCE_USERNAME);
        this.password = parameterTool.get(JobParamName.DATASOURCE_PASSWORD);
    }

    public Connection getConnection() {
        try {
            Class.forName("org.postgresql.Driver");
            connection = DriverManager
                    .getConnection(url, user, password);
            return connection;
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        return null;
    }
    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
