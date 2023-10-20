package com.clickzetta.jdbc_stress_tool;

import com.clickzetta.client.jdbc.core.CZJobMetric;
import com.clickzetta.client.jdbc.core.CZRequestIdGenerator;
import com.clickzetta.client.jdbc.core.CZStatement;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class CZSqlRunner extends SqlRunner {

    public CZSqlRunner(CompositeDataSource cds, String sqlId, String sql, String batchId) {
        super(cds, sqlId, sql, batchId);
    }

    public CZSqlRunner clone(String newSqlId, String newSql, String batchId) {
        return new CZSqlRunner(cds, newSqlId, newSql, batchId);
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
            CZStatement czStatement = cds.castToCZStatement(statement);

            long startTime = System.currentTimeMillis();
            metric.setClientStartMs(startTime);
            long resultSize = 0L;

            StringBuilder sb = new StringBuilder();
            String seperator = "";
            int submitted = 0;
            boolean hasResult;
            for (String q : SqlUtils.splitSql(sql)) {
                if (SqlUtils.isLocal(q)) { // no need to submit, eg. set x=y;
                    czStatement.execute(q);
                } else if (StringUtils.isNotEmpty(q.trim())) { // run at server side
                    String jobId = genJobId();
                    sb.append(seperator).append(jobId);
                    seperator = ":";
                    submitted++;
                    hasResult = czStatement.execute(q, jobId);
                    metric.setClientResultMs(System.currentTimeMillis());
                    if (hasResult) {
                        ResultSet rs = statement.getResultSet();
                        while (rs.next()) {
                            resultSize++;
                        }
                    }
                }
            }
            long endTime = System.currentTimeMillis();
            metric.setJobId(sb.toString());
            metric.setResultSize(resultSize);
            metric.setClientEndMs(endTime);
            metric.setSuccess(true);
            if (submitted == 1) {
                fillJobProfiling(metric, czStatement);
            }
        } catch (Throwable e) {
            long endTime = System.currentTimeMillis();
            metric.setClientEndMs(endTime);
            metric.setServerEndMs(endTime);
            System.err.println("failed to run sql '" + sqlId + "', reason: " + e.getMessage());
        } finally {
            // 释放资源
            close(connection);
        }
        return metric;
    }

    private String genJobId() {
        String jobId = CZRequestIdGenerator.getInstance().generate();
        if (jobIdPrefix != null && !jobIdPrefix.isEmpty()) {
            jobId = jobIdPrefix + "_" + jobId;
        }
        return jobId;
    }

    void fillJobProfiling(Metric metric, CZStatement czStatement) {
        CZJobMetric jobMetric = czStatement.getJobMetric();
        metric.setClientRequestMs(jobMetric.getClientRequestMs());
        metric.setClientResponseMs(jobMetric.getClientResponseMs());
        metric.setGatewayStartMs(jobMetric.getGatewayStartMs());
        metric.setGatewayEndMs(jobMetric.getGatewayEndMs());
        metric.setServerSubmitMs(jobMetric.getServerSubmitMs());
        metric.setServerStartMs(jobMetric.getServerStartMs());
        metric.setServerPlanMs(jobMetric.getServerPlanMs());
        metric.setServerDagMs(jobMetric.getServerDagMs());
        metric.setServerResourceMs(jobMetric.getServerResourceMs());
        metric.setServerEndMs(jobMetric.getServerEndMs());
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
}
