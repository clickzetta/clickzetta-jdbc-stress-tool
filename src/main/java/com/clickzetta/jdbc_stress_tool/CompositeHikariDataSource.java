package com.clickzetta.jdbc_stress_tool;

import com.clickzetta.client.jdbc.core.CZStatement;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.pool.HikariProxyStatement;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.sql.Statement;

public class CompositeHikariDataSource implements CompositeDataSource {

    HikariDataSource ds;

    public CompositeHikariDataSource(Config config) {
        HikariConfig hc = new HikariConfig();
        if (StringUtils.isNotEmpty(config.driverClass)) {
            hc.setDriverClassName(config.driverClass);
        }
        hc.setJdbcUrl(config.jdbcUrl);
        hc.setUsername(config.username);
        hc.setPassword(config.password);
        hc.setMaximumPoolSize(config.threadCount);
        hc.addDataSourceProperty("cachePrepStmts", "true");
        hc.addDataSourceProperty("prepStmtCacheSize", "100");
        hc.setConnectionInitSql(config.initSql);
        ds = new HikariDataSource(hc);
    }

    public HikariDataSource getDataSource() {
        return ds;
    }

    public int getActiveConnections() {
        return ds.getHikariPoolMXBean().getActiveConnections();
    }

    public int getTotalConnections() {
        return ds.getHikariPoolMXBean().getTotalConnections();
    }

    public CZStatement castToCZStatement(Statement statement) throws SQLException {
        return statement.unwrap(CZStatement.class);
    }

}
