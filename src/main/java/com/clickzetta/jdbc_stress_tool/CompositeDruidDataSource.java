package com.clickzetta.jdbc_stress_tool;

import com.alibaba.druid.pool.DruidDataSource;
import com.clickzetta.client.jdbc.core.CZStatement;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;

public class CompositeDruidDataSource implements CompositeDataSource {

    DruidDataSource ds;

    public CompositeDruidDataSource(Config config) {
        ds = new DruidDataSource();
        if (StringUtils.isNotEmpty(config.driverClass)) {
            ds.setDriverClassName(config.driverClass);
        }
        ds.setUrl(config.jdbcUrl);
        ds.setUsername(config.username);
        ds.setPassword(config.password);
        ds.setMaxActive(config.threadCount);
        ds.setInitialSize(config.threadCount);
        ds.setMinIdle(config.threadCount);
        ds.setMaxWait(10000);
        ds.setConnectionInitSqls(Collections.singletonList(config.initSql));
    }

    public DruidDataSource getDataSource() {
        return ds;
    }

    public int getActiveConnections() {
        return ds.getActiveCount();
    }

    public int getTotalConnections() {
        return ds.getPoolingCount();
    }

    public CZStatement castToCZStatement(Statement statement) throws SQLException {
        return statement.unwrap(CZStatement.class);
    }

}
