package com.clickzetta.jdbc_stress_tool;

public class CompositeDataSourceFactory {

    public static CompositeDataSource create(Config config) {
        if (config.connectionPoolType == Config.ConnectionPoolType.HIKARI) {
            return new CompositeHikariDataSource(config);
        } else if (config.connectionPoolType == Config.ConnectionPoolType.DBCP) {
            return new CompositeDBCPDataSource(config);
        } else if (config.connectionPoolType == Config.ConnectionPoolType.DRUID) {
            return new CompositeDruidDataSource(config);
        }
        throw new IllegalArgumentException("unknown connection pool type: " + config.connectionPoolType);
    }

}
