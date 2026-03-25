package com.clickzetta.jdbc_stress_tool;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class MetricTest {

    private Metric metric;

    @Before
    public void setUp() {
        metric = new Metric();
    }

    @Test
    public void testDefaultValues() {
        assertEquals("anonymous", metric.getJobId());
        assertFalse(metric.isSuccess());
        assertEquals(Long.valueOf(-1L), metric.getResultSize());
    }

    @Test
    public void testGetClientDuration() {
        metric.setClientStartMs(1000);
        metric.setClientEndMs(1500);
        assertEquals(Long.valueOf(500), metric.getClientDuration());
    }

    @Test
    public void testGetClientDuration_zero() {
        metric.setClientStartMs(1000);
        metric.setClientEndMs(1000);
        assertEquals(Long.valueOf(0), metric.getClientDuration());
    }

    @Test
    public void testHeaderContainsExpectedColumns() {
        String header = Metric.getHeader();
        assertTrue(header.contains("thread_name"));
        assertTrue(header.contains("sql_id"));
        assertTrue(header.contains("is_success"));
        assertTrue(header.contains("result_size"));
        assertTrue(header.contains("job_id"));
        assertTrue(header.contains("client_duration_ms"));
        assertTrue(header.contains("server_duration_ms"));
        assertTrue(header.contains("client_start_ms"));
        assertTrue(header.contains("client_end_ms"));
        assertTrue(header.contains("server_submit_ms"));
        assertTrue(header.contains("server_start_ms"));
        assertTrue(header.contains("server_end_ms"));
    }

    @Test
    public void testHeaderColumnCount() {
        String header = Metric.getHeader();
        String[] cols = header.split(",");
        assertEquals(24, cols.length);
    }

    @Test
    public void testToString_matchesHeaderColumnCount() {
        metric.setThreadName("t-1");
        metric.setSqlId("q1.sql");
        metric.setJobId("job-1");
        metric.setSuccess(true);
        metric.setResultSize(42L);
        metric.setClientStartMs(1000);
        metric.setClientEndMs(2000);
        metric.setClientRequestMs(1010);
        metric.setClientResponseMs(1990);
        metric.setClientResultMs(1500);
        metric.setGatewayStartMs(1020);
        metric.setGatewayEndMs(1980);
        metric.setServerSubmitMs(1030);
        metric.setServerStartMs(1040);
        metric.setServerPlanMs(1050);
        metric.setServerDagMs(1060);
        metric.setServerResourceMs(1070);
        metric.setServerEndMs(1970);
        metric.setPreCheckStartMs(1031);
        metric.setPreCheckEndMs(1035);
        metric.setSqlStartRunMs(1041);
        metric.setPlanCreatedMs(1051);
        metric.setGotResourcedMs(1071);
        metric.setDagCompletedMs(1960);

        String line = metric.toString();
        String[] parts = line.split(",");
        String[] headerParts = Metric.getHeader().split(",");
        assertEquals(headerParts.length, parts.length);
    }

    @Test
    public void testToString_containsCorrectValues() {
        metric.setThreadName("worker-0");
        metric.setSqlId("test.sql");
        metric.setSuccess(true);
        metric.setResultSize(100L);
        metric.setClientStartMs(5000);
        metric.setClientEndMs(6000);
        metric.setServerStartMs(5100);
        metric.setServerEndMs(5900);

        String line = metric.toString();
        assertTrue(line.startsWith("worker-0,test.sql,true,100,"));
        // client_duration = 6000 - 5000 = 1000
        assertTrue(line.contains(",1000,"));
        // server_duration = 5900 - 5100 = 800
        assertTrue(line.contains(",800,"));
    }

    @Test
    public void testSettersAndGetters() {
        metric.setThreadName("t");
        assertEquals("t", metric.getThreadName());

        metric.setSqlId("s");
        assertEquals("s", metric.getSqlId());

        metric.setJobId("j");
        assertEquals("j", metric.getJobId());

        metric.setSuccess(true);
        assertTrue(metric.isSuccess());

        metric.setResultSize(999L);
        assertEquals(Long.valueOf(999L), metric.getResultSize());
    }

    @Test
    public void testTimestampSettersAndGetters() {
        long ts = System.currentTimeMillis();
        metric.setClientStartMs(ts);
        assertEquals(ts, metric.getClientStartMs());
        metric.setClientEndMs(ts + 1);
        assertEquals(ts + 1, metric.getClientEndMs());
        metric.setServerSubmitMs(ts + 2);
        assertEquals(ts + 2, metric.getServerSubmitMs());
        metric.setServerStartMs(ts + 3);
        assertEquals(ts + 3, metric.getServerStartMs());
        metric.setServerEndMs(ts + 4);
        assertEquals(ts + 4, metric.getServerEndMs());
        metric.setGatewayStartMs(ts + 5);
        assertEquals(ts + 5, metric.getGatewayStartMs());
        metric.setGatewayEndMs(ts + 6);
        assertEquals(ts + 6, metric.getGatewayEndMs());
    }
}
