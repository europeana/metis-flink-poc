package eu.europeana.cloud.tool;


import eu.europeana.cloud.flink.client.constants.postgres.JobParamName;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;

public class DbConnectionProvider implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DbConnectionProvider.class);

    private final String url;
    private final String user;
    private final String password;


    public DbConnectionProvider(ParameterTool parameterTool) {
        this.url = parameterTool.getRequired(JobParamName.DATASOURCE_URL);
        this.user = parameterTool.get(JobParamName.DATASOURCE_USERNAME);
        this.password = parameterTool.get(JobParamName.DATASOURCE_PASSWORD);
    }

    public Connection getConnection() {
        try {
            return DriverManager
                    .getConnection(url, user, password);
        } catch (Exception e) {
            LOGGER.error("Could not connect to PostgreSQL database", e);
            System.exit(0);
        }
        return null;
    }
}
