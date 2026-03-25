package com.clickzetta.jdbc_stress_tool;

import com.clickzetta.client.jdbc.core.CZJobMetric;
import com.clickzetta.client.jdbc.core.CZRequestIdGenerator;
import com.clickzetta.client.jdbc.core.CZStatement;
import cz.proto.JobProfiling;
import cz.proto.coordinator.CoordinatorServiceOuterClass;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import javax.sql.DataSource;
import java.sql.*;
import java.util.Arrays;
import java.util.Collections;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class CZSqlRunnerTest {
    @Mock private CompositeDataSource mockCds;
    @Mock private DataSource mockDs;
    @Mock private Connection mockConn;
    @Mock private Statement mockStmt;
    @Mock private CZStatement mockCzStmt;
    @Mock private ResultSet mockRs;
    @Mock private CZRequestIdGenerator mockIdGen;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        when(mockCds.getDataSource()).thenReturn(mockDs);
        when(mockDs.getConnection()).thenReturn(mockConn);
        when(mockConn.createStatement()).thenReturn(mockStmt);
        when(mockCds.castToCZStatement(mockStmt)).thenReturn(mockCzStmt);
    }

    private void stubEmptyProfiling() {
        CZJobMetric jm = mock(CZJobMetric.class);
        when(mockCzStmt.getJobMetric()).thenReturn(jm);
        CoordinatorServiceOuterClass.JobStatus js = mock(CoordinatorServiceOuterClass.JobStatus.class);
        when(js.getJobProfiling()).thenReturn(JobProfiling.newBuilder().build());
        when(mockCzStmt.getJobStatus()).thenReturn(js);
    }

    // ── clone ───────────────────────────────────────────────────
    @Test
    public void testClone_returnsCZSqlRunner() {
        CZSqlRunner r = new CZSqlRunner(mockCds, "q1", "S1;", "b1");
        assertTrue(r.clone("q2", "S2;", "b2") instanceof CZSqlRunner);
    }
    @Test
    public void testClone_copiesFields() {
        CZSqlRunner r = new CZSqlRunner(mockCds, "q1", "S1;", "b1");
        CZSqlRunner c = r.clone("q2", "S2;", "b2");
        assertEquals("q2", c.sqlId);
        assertEquals("S2;", c.sql);
        assertEquals("b2", c.jobIdPrefix);
        assertNotSame(r, c);
    }
    @Test
    public void testClone_sharesSameDataSource() {
        CZSqlRunner r = new CZSqlRunner(mockCds, "q1", "S1;", "b");
        assertSame(r.cds, r.clone("q2", "S2;", "b").cds);
    }

    // ── call: single remote SQL, success with result rows ───────
    @Test
    public void testCall_singleRemote_success() throws Exception {
        try (MockedStatic<SqlUtils> su = mockStatic(SqlUtils.class);
             MockedStatic<CZRequestIdGenerator> ig = mockStatic(CZRequestIdGenerator.class)) {
            su.when(() -> SqlUtils.splitSql("SELECT 1;")).thenReturn(Collections.singletonList("SELECT 1"));
            su.when(() -> SqlUtils.isLocal("SELECT 1")).thenReturn(false);
            ig.when(CZRequestIdGenerator::getInstance).thenReturn(mockIdGen);
            when(mockIdGen.generate()).thenReturn("job-abc");
            when(mockCzStmt.execute("SELECT 1", "job-abc")).thenReturn(true);
            when(mockStmt.getResultSet()).thenReturn(mockRs);
            when(mockRs.next()).thenReturn(true, true, false);
            CZJobMetric jm = mock(CZJobMetric.class);
            when(jm.getClientRequestMs()).thenReturn(100L);
            when(jm.getClientResponseMs()).thenReturn(200L);
            when(jm.getGatewayStartMs()).thenReturn(110L);
            when(jm.getGatewayEndMs()).thenReturn(190L);
            when(jm.getServerSubmitMs()).thenReturn(120L);
            when(jm.getServerStartMs()).thenReturn(130L);
            when(jm.getServerPlanMs()).thenReturn(140L);
            when(jm.getServerDagMs()).thenReturn(150L);
            when(jm.getServerResourceMs()).thenReturn(160L);
            when(jm.getServerEndMs()).thenReturn(180L);
            when(mockCzStmt.getJobMetric()).thenReturn(jm);
            CoordinatorServiceOuterClass.JobStatus js = mock(CoordinatorServiceOuterClass.JobStatus.class);
            when(js.getJobProfiling()).thenReturn(JobProfiling.newBuilder().build());
            when(mockCzStmt.getJobStatus()).thenReturn(js);
            CZSqlRunner r = new CZSqlRunner(mockCds, "q1.sql", "SELECT 1;", "");
            Metric m = r.call();
            assertTrue(m.isSuccess());
            assertEquals(Long.valueOf(2L), m.getResultSize());
            assertEquals("job-abc", m.getJobId());
            assertTrue(m.getClientEndMs() >= m.getClientStartMs());
            // fillJobProfiling called (submitted==1)
            assertEquals(100L, m.getClientRequestMs());
            assertEquals(200L, m.getClientResponseMs());
            assertEquals(180L, m.getServerEndMs());
        }
    }

    // ── call: single remote, no result set ──────────────────────
    @Test
    public void testCall_singleRemote_noResultSet() throws Exception {
        try (MockedStatic<SqlUtils> su = mockStatic(SqlUtils.class);
             MockedStatic<CZRequestIdGenerator> ig = mockStatic(CZRequestIdGenerator.class)) {
            su.when(() -> SqlUtils.splitSql("INSERT 1;")).thenReturn(Collections.singletonList("INSERT 1"));
            su.when(() -> SqlUtils.isLocal("INSERT 1")).thenReturn(false);
            ig.when(CZRequestIdGenerator::getInstance).thenReturn(mockIdGen);
            when(mockIdGen.generate()).thenReturn("job-ins");
            when(mockCzStmt.execute("INSERT 1", "job-ins")).thenReturn(false);
            stubEmptyProfiling();
            CZSqlRunner r = new CZSqlRunner(mockCds, "ins.sql", "INSERT 1;", "");
            Metric m = r.call();
            assertTrue(m.isSuccess());
            assertEquals(Long.valueOf(0L), m.getResultSize());
            assertEquals("job-ins", m.getJobId());
        }
    }

    // ── call: local SQL only ────────────────────────────────────
    @Test
    public void testCall_localOnly_noJobSubmitted() throws Exception {
        try (MockedStatic<SqlUtils> su = mockStatic(SqlUtils.class)) {
            su.when(() -> SqlUtils.splitSql("set x=1;")).thenReturn(Collections.singletonList("set x=1"));
            su.when(() -> SqlUtils.isLocal("set x=1")).thenReturn(true);
            CZSqlRunner r = new CZSqlRunner(mockCds, "local.sql", "set x=1;", "");
            Metric m = r.call();
            assertTrue(m.isSuccess());
            assertEquals(Long.valueOf(0L), m.getResultSize());
            assertEquals("", m.getJobId());
            verify(mockCzStmt).execute("set x=1");
            verify(mockCzStmt, never()).execute(anyString(), anyString());
        }
    }

    // ── call: mixed local + remote ──────────────────────────────
    @Test
    public void testCall_mixedLocalAndRemote() throws Exception {
        try (MockedStatic<SqlUtils> su = mockStatic(SqlUtils.class);
             MockedStatic<CZRequestIdGenerator> ig = mockStatic(CZRequestIdGenerator.class)) {
            su.when(() -> SqlUtils.splitSql("set x=1;SELECT 1;")).thenReturn(Arrays.asList("set x=1", "SELECT 1"));
            su.when(() -> SqlUtils.isLocal("set x=1")).thenReturn(true);
            su.when(() -> SqlUtils.isLocal("SELECT 1")).thenReturn(false);
            ig.when(CZRequestIdGenerator::getInstance).thenReturn(mockIdGen);
            when(mockIdGen.generate()).thenReturn("job-mix");
            when(mockCzStmt.execute("SELECT 1", "job-mix")).thenReturn(false);
            stubEmptyProfiling();
            CZSqlRunner r = new CZSqlRunner(mockCds, "mix.sql", "set x=1;SELECT 1;", "");
            Metric m = r.call();
            assertTrue(m.isSuccess());
            assertEquals("job-mix", m.getJobId());
            verify(mockCzStmt).execute("set x=1");
            verify(mockCzStmt).execute("SELECT 1", "job-mix");
        }
    }

    // ── call: multiple remote → no fillJobProfiling ─────────────
    @Test
    public void testCall_multiRemote_noFillProfiling() throws Exception {
        try (MockedStatic<SqlUtils> su = mockStatic(SqlUtils.class);
             MockedStatic<CZRequestIdGenerator> ig = mockStatic(CZRequestIdGenerator.class)) {
            su.when(() -> SqlUtils.splitSql("S1;S2;")).thenReturn(Arrays.asList("S1", "S2"));
            su.when(() -> SqlUtils.isLocal("S1")).thenReturn(false);
            su.when(() -> SqlUtils.isLocal("S2")).thenReturn(false);
            ig.when(CZRequestIdGenerator::getInstance).thenReturn(mockIdGen);
            when(mockIdGen.generate()).thenReturn("j1", "j2");
            when(mockCzStmt.execute("S1", "j1")).thenReturn(false);
            when(mockCzStmt.execute("S2", "j2")).thenReturn(false);
            CZSqlRunner r = new CZSqlRunner(mockCds, "m.sql", "S1;S2;", "");
            Metric m = r.call();
            assertTrue(m.isSuccess());
            assertEquals("j1:j2", m.getJobId());
            verify(mockCzStmt, never()).getJobMetric();
        }
    }

    // ── call: three remote → colon-separated jobIds ─────────────
    @Test
    public void testCall_threeRemote_jobIdConcat() throws Exception {
        try (MockedStatic<SqlUtils> su = mockStatic(SqlUtils.class);
             MockedStatic<CZRequestIdGenerator> ig = mockStatic(CZRequestIdGenerator.class)) {
            su.when(() -> SqlUtils.splitSql("A;B;C;")).thenReturn(Arrays.asList("A", "B", "C"));
            su.when(() -> SqlUtils.isLocal(anyString())).thenReturn(false);
            ig.when(CZRequestIdGenerator::getInstance).thenReturn(mockIdGen);
            when(mockIdGen.generate()).thenReturn("a", "b", "c");
            when(mockCzStmt.execute(anyString(), anyString())).thenReturn(false);
            CZSqlRunner r = new CZSqlRunner(mockCds, "t.sql", "A;B;C;", "");
            assertEquals("a:b:c", r.call().getJobId());
        }
    }

    // ── genJobId: with prefix ───────────────────────────────────
    @Test
    public void testCall_genJobId_withPrefix() throws Exception {
        try (MockedStatic<SqlUtils> su = mockStatic(SqlUtils.class);
             MockedStatic<CZRequestIdGenerator> ig = mockStatic(CZRequestIdGenerator.class)) {
            su.when(() -> SqlUtils.splitSql("S;")).thenReturn(Collections.singletonList("S"));
            su.when(() -> SqlUtils.isLocal("S")).thenReturn(false);
            ig.when(CZRequestIdGenerator::getInstance).thenReturn(mockIdGen);
            when(mockIdGen.generate()).thenReturn("raw-999");
            when(mockCzStmt.execute(eq("S"), eq("pfx_raw-999"))).thenReturn(false);
            stubEmptyProfiling();
            CZSqlRunner r = new CZSqlRunner(mockCds, "q.sql", "S;", "pfx");
            assertEquals("pfx_raw-999", r.call().getJobId());
        }
    }

    // ── genJobId: empty prefix ──────────────────────────────────
    @Test
    public void testCall_genJobId_emptyPrefix() throws Exception {
        try (MockedStatic<SqlUtils> su = mockStatic(SqlUtils.class);
             MockedStatic<CZRequestIdGenerator> ig = mockStatic(CZRequestIdGenerator.class)) {
            su.when(() -> SqlUtils.splitSql("S;")).thenReturn(Collections.singletonList("S"));
            su.when(() -> SqlUtils.isLocal("S")).thenReturn(false);
            ig.when(CZRequestIdGenerator::getInstance).thenReturn(mockIdGen);
            when(mockIdGen.generate()).thenReturn("raw-000");
            when(mockCzStmt.execute(eq("S"), eq("raw-000"))).thenReturn(false);
            stubEmptyProfiling();
            CZSqlRunner r = new CZSqlRunner(mockCds, "q.sql", "S;", "");
            assertEquals("raw-000", r.call().getJobId());
        }
    }

    // ── genJobId: null prefix ───────────────────────────────────
    @Test
    public void testCall_genJobId_nullPrefix() throws Exception {
        try (MockedStatic<SqlUtils> su = mockStatic(SqlUtils.class);
             MockedStatic<CZRequestIdGenerator> ig = mockStatic(CZRequestIdGenerator.class)) {
            su.when(() -> SqlUtils.splitSql("S;")).thenReturn(Collections.singletonList("S"));
            su.when(() -> SqlUtils.isLocal("S")).thenReturn(false);
            ig.when(CZRequestIdGenerator::getInstance).thenReturn(mockIdGen);
            when(mockIdGen.generate()).thenReturn("raw-null");
            when(mockCzStmt.execute(eq("S"), eq("raw-null"))).thenReturn(false);
            stubEmptyProfiling();
            CZSqlRunner r = new CZSqlRunner(mockCds, "q.sql", "S;", null);
            assertEquals("raw-null", r.call().getJobId());
        }
    }

    // ── call: SQL exception ─────────────────────────────────────
    @Test
    public void testCall_sqlException_setsFailure() throws Exception {
        try (MockedStatic<SqlUtils> su = mockStatic(SqlUtils.class);
             MockedStatic<CZRequestIdGenerator> ig = mockStatic(CZRequestIdGenerator.class)) {
            su.when(() -> SqlUtils.splitSql("BAD;")).thenReturn(Collections.singletonList("BAD"));
            su.when(() -> SqlUtils.isLocal("BAD")).thenReturn(false);
            ig.when(CZRequestIdGenerator::getInstance).thenReturn(mockIdGen);
            when(mockIdGen.generate()).thenReturn("j-fail");
            when(mockCzStmt.execute("BAD", "j-fail")).thenThrow(new SQLException("syntax error"));
            CZSqlRunner r = new CZSqlRunner(mockCds, "bad.sql", "BAD;", "");
            Metric m = r.call();
            assertFalse(m.isSuccess());
            assertTrue(m.getClientEndMs() > 0);
            assertTrue(m.getServerEndMs() > 0);
            assertEquals("bad.sql", m.getJobId());
        }
    }

    // ── call: connection exception ──────────────────────────────
    @Test
    public void testCall_connectionException() throws Exception {
        when(mockDs.getConnection()).thenThrow(new SQLException("refused"));
        CZSqlRunner r = new CZSqlRunner(mockCds, "c.sql", "S;", "");
        Metric m = r.call();
        assertFalse(m.isSuccess());
        assertTrue(m.getClientEndMs() > 0);
    }

    // ── call: castToCZStatement exception ────────────────────────
    @Test
    public void testCall_castException() throws Exception {
        when(mockCds.castToCZStatement(mockStmt)).thenThrow(new SQLException("not CZ"));
        CZSqlRunner r = new CZSqlRunner(mockCds, "x.sql", "S;", "");
        assertFalse(r.call().isSuccess());
    }

    // ── call: empty/whitespace segments skipped ─────────────────
    @Test
    public void testCall_emptySegmentsSkipped() throws Exception {
        try (MockedStatic<SqlUtils> su = mockStatic(SqlUtils.class);
             MockedStatic<CZRequestIdGenerator> ig = mockStatic(CZRequestIdGenerator.class)) {
            su.when(() -> SqlUtils.splitSql("S;;")).thenReturn(Arrays.asList("S", "", "  "));
            su.when(() -> SqlUtils.isLocal("S")).thenReturn(false);
            su.when(() -> SqlUtils.isLocal("")).thenReturn(false);
            su.when(() -> SqlUtils.isLocal("  ")).thenReturn(false);
            ig.when(CZRequestIdGenerator::getInstance).thenReturn(mockIdGen);
            when(mockIdGen.generate()).thenReturn("j-only");
            when(mockCzStmt.execute("S", "j-only")).thenReturn(false);
            stubEmptyProfiling();
            CZSqlRunner r = new CZSqlRunner(mockCds, "e.sql", "S;;", "");
            Metric m = r.call();
            assertTrue(m.isSuccess());
            assertEquals("j-only", m.getJobId());
            verify(mockCzStmt, times(1)).execute(anyString(), anyString());
        }
    }

    // ── call: sets threadName and sqlId ──────────────────────────
    @Test
    public void testCall_setsThreadNameAndSqlId() throws Exception {
        try (MockedStatic<SqlUtils> su = mockStatic(SqlUtils.class)) {
            su.when(() -> SqlUtils.splitSql("set a=b;")).thenReturn(Collections.singletonList("set a=b"));
            su.when(() -> SqlUtils.isLocal("set a=b")).thenReturn(true);
            CZSqlRunner r = new CZSqlRunner(mockCds, "my.sql", "set a=b;", "");
            Metric m = r.call();
            assertEquals(Thread.currentThread().getName(), m.getThreadName());
            assertEquals("my.sql", m.getSqlId());
        }
    }

    // ── fillJobProfiling: all 6 event codes ─────────────────────
    @Test
    public void testFillJobProfiling_allEventCodes() {
        CZJobMetric jm = mock(CZJobMetric.class);
        when(jm.getClientRequestMs()).thenReturn(100L);
        when(jm.getClientResponseMs()).thenReturn(200L);
        when(jm.getGatewayStartMs()).thenReturn(110L);
        when(jm.getGatewayEndMs()).thenReturn(190L);
        when(jm.getServerSubmitMs()).thenReturn(120L);
        when(jm.getServerStartMs()).thenReturn(130L);
        when(jm.getServerPlanMs()).thenReturn(140L);
        when(jm.getServerDagMs()).thenReturn(150L);
        when(jm.getServerResourceMs()).thenReturn(160L);
        when(jm.getServerEndMs()).thenReturn(180L);
        when(mockCzStmt.getJobMetric()).thenReturn(jm);
        JobProfiling p = JobProfiling.newBuilder()
            .addProfiling(JobProfiling.JobProfilingItem.newBuilder().setE(101).setT(1001L).build())
            .addProfiling(JobProfiling.JobProfilingItem.newBuilder().setE(102).setT(1002L).build())
            .addProfiling(JobProfiling.JobProfilingItem.newBuilder().setE(106).setT(1006L).build())
            .addProfiling(JobProfiling.JobProfilingItem.newBuilder().setE(108).setT(1008L).build())
            .addProfiling(JobProfiling.JobProfilingItem.newBuilder().setE(120).setT(1020L).build())
            .addProfiling(JobProfiling.JobProfilingItem.newBuilder().setE(130).setT(1030L).build())
            .build();
        CoordinatorServiceOuterClass.JobStatus js = mock(CoordinatorServiceOuterClass.JobStatus.class);
        when(js.getJobProfiling()).thenReturn(p);
        when(mockCzStmt.getJobStatus()).thenReturn(js);
        Metric m = new Metric();
        new CZSqlRunner(mockCds, "q", "S;", "").fillJobProfiling(m, mockCzStmt);
        assertEquals(100L, m.getClientRequestMs());
        assertEquals(200L, m.getClientResponseMs());
        assertEquals(110L, m.getGatewayStartMs());
        assertEquals(190L, m.getGatewayEndMs());
        assertEquals(120L, m.getServerSubmitMs());
        assertEquals(130L, m.getServerStartMs());
        assertEquals(140L, m.getServerPlanMs());
        assertEquals(150L, m.getServerDagMs());
        assertEquals(160L, m.getServerResourceMs());
        assertEquals(180L, m.getServerEndMs());
        assertEquals(1001L, m.getPreCheckStartMs());
        assertEquals(1002L, m.getPreCheckEndMs());
        assertEquals(1006L, m.getSqlStartRunMs());
        assertEquals(1008L, m.getPlanCreatedMs());
        assertEquals(1020L, m.getGotResourcedMs());
        assertEquals(1030L, m.getDagCompletedMs());
    }

    // ── fillJobProfiling: unknown event codes ignored ───────────
    @Test
    public void testFillJobProfiling_unknownCodesIgnored() {
        CZJobMetric jm = mock(CZJobMetric.class);
        when(mockCzStmt.getJobMetric()).thenReturn(jm);
        JobProfiling p = JobProfiling.newBuilder()
            .addProfiling(JobProfiling.JobProfilingItem.newBuilder().setE(999).setT(9999L).build())
            .addProfiling(JobProfiling.JobProfilingItem.newBuilder().setE(200).setT(2000L).build())
            .build();
        CoordinatorServiceOuterClass.JobStatus js = mock(CoordinatorServiceOuterClass.JobStatus.class);
        when(js.getJobProfiling()).thenReturn(p);
        when(mockCzStmt.getJobStatus()).thenReturn(js);
        Metric m = new Metric();
        new CZSqlRunner(mockCds, "q", "S;", "").fillJobProfiling(m, mockCzStmt);
        assertEquals(0L, m.getPreCheckStartMs());
        assertEquals(0L, m.getPreCheckEndMs());
        assertEquals(0L, m.getSqlStartRunMs());
        assertEquals(0L, m.getPlanCreatedMs());
        assertEquals(0L, m.getGotResourcedMs());
        assertEquals(0L, m.getDagCompletedMs());
    }

    // ── fillJobProfiling: empty profiling list ──────────────────
    @Test
    public void testFillJobProfiling_empty() {
        CZJobMetric jm = mock(CZJobMetric.class);
        when(mockCzStmt.getJobMetric()).thenReturn(jm);
        CoordinatorServiceOuterClass.JobStatus js = mock(CoordinatorServiceOuterClass.JobStatus.class);
        when(js.getJobProfiling()).thenReturn(JobProfiling.newBuilder().build());
        when(mockCzStmt.getJobStatus()).thenReturn(js);
        Metric m = new Metric();
        new CZSqlRunner(mockCds, "q", "S;", "").fillJobProfiling(m, mockCzStmt);
        assertEquals(0L, m.getPreCheckStartMs());
    }

    // ── fillJobProfiling: partial event codes ───────────────────
    @Test
    public void testFillJobProfiling_partial() {
        CZJobMetric jm = mock(CZJobMetric.class);
        when(mockCzStmt.getJobMetric()).thenReturn(jm);
        JobProfiling p = JobProfiling.newBuilder()
            .addProfiling(JobProfiling.JobProfilingItem.newBuilder().setE(101).setT(500L).build())
            .addProfiling(JobProfiling.JobProfilingItem.newBuilder().setE(130).setT(900L).build())
            .build();
        CoordinatorServiceOuterClass.JobStatus js = mock(CoordinatorServiceOuterClass.JobStatus.class);
        when(js.getJobProfiling()).thenReturn(p);
        when(mockCzStmt.getJobStatus()).thenReturn(js);
        Metric m = new Metric();
        new CZSqlRunner(mockCds, "q", "S;", "").fillJobProfiling(m, mockCzStmt);
        assertEquals(500L, m.getPreCheckStartMs());
        assertEquals(0L, m.getPreCheckEndMs());
        assertEquals(0L, m.getSqlStartRunMs());
        assertEquals(0L, m.getPlanCreatedMs());
        assertEquals(0L, m.getGotResourcedMs());
        assertEquals(900L, m.getDagCompletedMs());
    }

    // ── call: result count across multiple remote SQLs ──────────
    @Test
    public void testCall_resultCountAcrossMultipleRemote() throws Exception {
        try (MockedStatic<SqlUtils> su = mockStatic(SqlUtils.class);
             MockedStatic<CZRequestIdGenerator> ig = mockStatic(CZRequestIdGenerator.class)) {
            su.when(() -> SqlUtils.splitSql("S1;S2;")).thenReturn(Arrays.asList("S1", "S2"));
            su.when(() -> SqlUtils.isLocal(anyString())).thenReturn(false);
            ig.when(CZRequestIdGenerator::getInstance).thenReturn(mockIdGen);
            when(mockIdGen.generate()).thenReturn("j1", "j2");
            ResultSet rs1 = mock(ResultSet.class);
            ResultSet rs2 = mock(ResultSet.class);
            when(mockCzStmt.execute("S1", "j1")).thenReturn(true);
            when(mockCzStmt.execute("S2", "j2")).thenReturn(true);
            when(mockStmt.getResultSet()).thenReturn(rs1, rs2);
            when(rs1.next()).thenReturn(true, true, false);
            when(rs2.next()).thenReturn(true, true, true, false);
            CZSqlRunner r = new CZSqlRunner(mockCds, "m.sql", "S1;S2;", "");
            Metric m = r.call();
            assertTrue(m.isSuccess());
            assertEquals(Long.valueOf(5L), m.getResultSize());
        }
    }

    // ── call: clientStartMs set before execution ────────────────
    @Test
    public void testCall_clientStartMsSetBeforeExec() throws Exception {
        try (MockedStatic<SqlUtils> su = mockStatic(SqlUtils.class)) {
            su.when(() -> SqlUtils.splitSql("set a=1;")).thenReturn(Collections.singletonList("set a=1"));
            su.when(() -> SqlUtils.isLocal("set a=1")).thenReturn(true);
            CZSqlRunner r = new CZSqlRunner(mockCds, "t.sql", "set a=1;", "");
            Metric m = r.call();
            assertTrue(m.getClientStartMs() > 0);
            assertTrue(m.getClientEndMs() >= m.getClientStartMs());
        }
    }
}
