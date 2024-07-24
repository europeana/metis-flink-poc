package eu.europeana.cloud.tool;


import eu.europeana.cloud.flink.client.constants.postgres.JobParamName;
import java.sql.SQLException;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;

public class DbConnectionProvider implements Serializable {

    private final String url;
    private final String user;
    private final String password;


    public DbConnectionProvider(ParameterTool parameterTool) {
        this.url = parameterTool.getRequired(JobParamName.DATASOURCE_URL);
        this.user = parameterTool.get(JobParamName.DATASOURCE_USERNAME);
        this.password = parameterTool.get(JobParamName.DATASOURCE_PASSWORD);
    }

    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }
}
