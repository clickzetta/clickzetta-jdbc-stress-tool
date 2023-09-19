package com.clickzetta.jdbc_stress_tool;

import org.apache.commons.lang3.StringUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Callable;

public class SqlRunner implements Callable<Metric> {
    String sqlId;
    String sql;
    String jobIdPrefix;
    String threadName = "";
    DataSource ds;
    CompositeDataSource cds;

    public SqlRunner(CompositeDataSource cds, String sqlId, String sql, String prefix) {
        this.cds = cds;
        this.ds = cds.getDataSource();
        this.sqlId = sqlId;
        this.sql = sql;
        this.jobIdPrefix = prefix;
    }

    public SqlRunner clone(String _sqlId, String _sql, String _prefix) {
        return new SqlRunner(cds, _sqlId, _sql, _prefix);
    }

    @Override
    public Metric call() throws Exception {
        this.threadName = Thread.currentThread().getName();
        Metric metric = new Metric();
        metric.setThreadName(threadName);
        metric.setSqlId(sqlId);
        metric.setJobId(sqlId);
        Connection connection = ds.getConnection();
        try {
            Statement statement = connection.createStatement();

            long startTime = System.currentTimeMillis();
            metric.setClientStartMs(startTime);
            metric.setServerSubmitMs(startTime);
            metric.setServerStartMs(startTime);

            long resultSize = 0L;

            for (String q : SqlUtils.splitSql(sql)) {
                if (StringUtils.isNotEmpty(q.trim())) {
                    if (statement.execute(q)) {
                        ResultSet rs = statement.getResultSet();
                        while (rs.next()) {
                            resultSize++;
                        }
                    }
                }
            }

            long endTime = System.currentTimeMillis();
            metric.setSuccess(true);
            metric.setResultSize(resultSize);
            metric.setClientEndMs(endTime);
            metric.setServerEndMs(endTime);
        } catch (Throwable e) {
            long endTime = System.currentTimeMillis();
            metric.setClientEndMs(endTime);
            metric.setServerEndMs(endTime);
            System.err.println("failed to run sql " + sqlId + ", reason: " + e.getMessage());
        } finally {
            close(connection);
        }
        return metric;
    }

    private static void close(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                System.err.println("failed to close connection, reason: " + e.getMessage());
            }
        }
    }

    public static void warmUpConnection(Connection conn, String sql) throws SQLException {
        Statement statement = conn.createStatement();
        if (statement.execute(sql)) {
            ResultSet rs = statement.getResultSet();
            while(rs.next()) {
                // pass
            }
            rs.close();
        }
        statement.close();
    }
}
