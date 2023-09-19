package com.clickzetta.jdbc_stress_tool;

import com.clickzetta.client.jdbc.core.CZStatement;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.sql.Statement;

public class CompositeDBCPDataSource implements CompositeDataSource {

    BasicDataSource ds;

    public CompositeDBCPDataSource(Config config) {
        ds = new BasicDataSource();
        if (StringUtils.isNotEmpty(config.driverClass)) {
            ds.setDriverClassName(config.driverClass);
        }
        ds.setUrl(config.jdbcUrl);
        ds.setUsername(config.username);
        ds.setPassword(config.password);
        ds.setMaxActive(config.threadCount);
        ds.setInitialSize(config.threadCount);
        ds.setMinIdle(config.threadCount);
        ds.setMaxIdle(config.threadCount);
        ds.setMaxWait(10000);
    }

    public BasicDataSource getDataSource() {
        return ds;
    }

    public int getActiveConnections() {
        return ds.getNumActive();
    }

    public int getTotalConnections() {
        return ds.getNumActive() + ds.getNumIdle();
    }

    public CZStatement castToCZStatement(Statement statement) throws SQLException {
        return statement.unwrap(CZStatement.class);
    }

}
