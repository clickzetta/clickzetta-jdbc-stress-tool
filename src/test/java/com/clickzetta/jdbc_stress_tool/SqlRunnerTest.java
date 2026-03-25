package com.clickzetta.jdbc_stress_tool;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.sql.DataSource;
import java.sql.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class SqlRunnerTest {

    @Mock private CompositeDataSource mockCds;
    @Mock private DataSource mockDs;
    @Mock private Connection mockConn;
    @Mock private Statement mockStmt;
    @Mock private ResultSet mockRs;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        when(mockCds.getDataSource()).thenReturn(mockDs);
    }

    // ── constructor & clone ─────────────────────────────────────────────────

    @Test
    public void testConstructor() {
        SqlRunner runner = new SqlRunner(mockCds, "q1.sql", "SELECT 1;", "prefix");
        assertEquals("q1.sql", runner.sqlId);
        assertEquals("SELECT 1;", runner.sql);
        assertEquals("prefix", runner.jobIdPrefix);
    }

    @Test
    public void testClone() {
        SqlRunner original = new SqlRunner(mockCds, "q1.sql", "SELECT 1;", "p");
        SqlRunner cloned = original.clone("q2.sql", "SELECT 2;", "p2");
        assertEquals("q2.sql", cloned.sqlId);
        assertEquals("SELECT 2;", cloned.sql);
        assertEquals("p2", cloned.jobIdPrefix);
        assertNotSame(original, cloned);
    }

    // ── call() ──────────────────────────────────────────────────────────────

    @Test
    public void testCall_successfulExecution() throws Exception {
        when(mockDs.getConnection()).thenReturn(mockConn);
        when(mockConn.createStatement()).thenReturn(mockStmt);
        when(mockStmt.execute("SELECT 1")).thenReturn(true);
        when(mockStmt.getResultSet()).thenReturn(mockRs);
        when(mockRs.next()).thenReturn(true, false); // one row

        SqlRunner runner = new SqlRunner(mockCds, "q1.sql", "SELECT 1;", "");
        Metric metric = runner.call();

        assertTrue(metric.isSuccess());
        assertEquals(Long.valueOf(1L), metric.getResultSize());
        assertEquals("q1.sql", metric.getSqlId());
        assertTrue(metric.getClientDuration() >= 0);
    }

    @Test
    public void testCall_noResultSet() throws Exception {
        when(mockDs.getConnection()).thenReturn(mockConn);
        when(mockConn.createStatement()).thenReturn(mockStmt);
        when(mockStmt.execute("INSERT INTO t VALUES(1)")).thenReturn(false);

        SqlRunner runner = new SqlRunner(mockCds, "insert.sql", "INSERT INTO t VALUES(1);", "");
        Metric metric = runner.call();

        assertTrue(metric.isSuccess());
        assertEquals(Long.valueOf(0L), metric.getResultSize());
    }

    @Test
    public void testCall_exceptionSetsFailure() throws Exception {
        when(mockDs.getConnection()).thenReturn(mockConn);
        when(mockConn.createStatement()).thenReturn(mockStmt);
        when(mockStmt.execute(anyString())).thenThrow(new SQLException("connection lost"));

        SqlRunner runner = new SqlRunner(mockCds, "fail.sql", "SELECT 1;", "");
        Metric metric = runner.call();

        assertFalse(metric.isSuccess());
        assertTrue(metric.getClientEndMs() > 0);
    }

    @Test
    public void testCall_setsThreadName() throws Exception {
        when(mockDs.getConnection()).thenReturn(mockConn);
        when(mockConn.createStatement()).thenReturn(mockStmt);
        when(mockStmt.execute(anyString())).thenReturn(false);

        SqlRunner runner = new SqlRunner(mockCds, "q.sql", "SELECT 1;", "");
        Metric metric = runner.call();

        assertNotNull(metric.getThreadName());
        assertFalse(metric.getThreadName().isEmpty());
    }

    @Test
    public void testCall_multipleResultRows() throws Exception {
        when(mockDs.getConnection()).thenReturn(mockConn);
        when(mockConn.createStatement()).thenReturn(mockStmt);
        when(mockStmt.execute("SELECT * FROM t")).thenReturn(true);
        when(mockStmt.getResultSet()).thenReturn(mockRs);
        when(mockRs.next()).thenReturn(true, true, true, false); // 3 rows

        SqlRunner runner = new SqlRunner(mockCds, "multi.sql", "SELECT * FROM t;", "");
        Metric metric = runner.call();

        assertTrue(metric.isSuccess());
        assertEquals(Long.valueOf(3L), metric.getResultSize());
    }

    // ── warmUpConnection ────────────────────────────────────────────────────

    @Test
    public void testWarmUpConnection_withResultSet() throws Exception {
        when(mockConn.createStatement()).thenReturn(mockStmt);
        when(mockStmt.execute("select 1")).thenReturn(true);
        when(mockStmt.getResultSet()).thenReturn(mockRs);
        when(mockRs.next()).thenReturn(false);

        SqlRunner.warmUpConnection(mockConn, "select 1;");

        verify(mockStmt).execute("select 1");
        verify(mockRs).close();
        verify(mockStmt).close();
    }

    @Test
    public void testWarmUpConnection_noResultSet() throws Exception {
        when(mockConn.createStatement()).thenReturn(mockStmt);
        when(mockStmt.execute("SET x=1")).thenReturn(false);

        SqlRunner.warmUpConnection(mockConn, "SET x=1;");

        verify(mockStmt).execute("SET x=1");
        verify(mockStmt).close();
    }
}
