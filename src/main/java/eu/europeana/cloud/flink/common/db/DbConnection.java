package eu.europeana.cloud.flink.common.db;


import org.apache.flink.api.java.utils.ParameterTool;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DbConnection implements Serializable {

    private Connection connection;
    private String url;
    private String user;
    private String password;


    public DbConnection(ParameterTool parameterTool) {
        this.url = parameterTool.getRequired("datasource.url");
        this.user = parameterTool.get("datasource.username");
        this.password = parameterTool.get("datasource.password");
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
