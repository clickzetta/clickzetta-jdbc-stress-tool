package com.clickzetta.jdbc_stress_tool;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.Statement;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Tests for Main.run() using reflection to bypass the constructor's real pool creation.
 */
public class MainTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Mock private CompositeDataSource mockCds;
    @Mock private DataSource mockDs;
    @Mock private Connection mockConn;
    @Mock private Statement mockStmt;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        when(mockCds.getDataSource()).thenReturn(mockDs);
        when(mockDs.getConnection()).thenReturn(mockConn);
        when(mockConn.createStatement()).thenReturn(mockStmt);
    }

    private Config makeValidConfig() throws IOException {
        Config config = new Config();
        config.setJdbcUrl("jdbc:postgresql://localhost/test");
        config.setUsername("sa");
        config.setPassword("pass");
        config.setThreadCount(1);
        config.setRepeatCount(1);
        config.setInitSql("select 1;");
        config.setOutput(tempFolder.newFile("out.csv").getAbsolutePath());
        config.getSqls().put("q1.sql", "SELECT 1;");
        config.setPrefix("");
        config.setFailureRate(10.0);
        return config;
    }

    /**
     * Create a Main instance with mocked internals via reflection,
     * bypassing the constructor that creates real connection pools.
     */
    private Main buildMockedMain(Config config) throws Exception {
        // Use objenesis-style: allocate via Unsafe to skip constructor
        Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
        f.setAccessible(true);
        sun.misc.Unsafe unsafe = (sun.misc.Unsafe) f.get(null);
        Main main = (Main) unsafe.allocateInstance(Main.class);

        // Set fields via reflection
        setField(main, "config", config);
        setField(main, "cds", mockCds);
        setField(main, "initSqlRunner", new SqlRunner(mockCds, "init", "select 1;", config.getPrefix()));
        return main;
    }

    private void setField(Object obj, String fieldName, Object value) throws Exception {
        Field field = obj.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(obj, value);
    }

    // ── run() ───────────────────────────────────────────────────────────────

    @Test
    public void testRun_writesOutputCsv() throws Exception {
        Config config = makeValidConfig();
        File outFile = tempFolder.newFile("run_out.csv");
        config.setOutput(outFile.getAbsolutePath());
        config.setRepeatCount(2);

        when(mockStmt.execute(anyString())).thenReturn(false);

        Main main = buildMockedMain(config);
        main.run();

        String content = new String(java.nio.file.Files.readAllBytes(outFile.toPath()));
        String[] lines = content.trim().split("\n");
        assertTrue("Should have header + data rows", lines.length >= 3);
        assertTrue(lines[0].contains("thread_name"));
    }

    @Test
    public void testRun_handlesFailedSqls() throws Exception {
        Config config = makeValidConfig();
        File outFile = tempFolder.newFile("fail_out.csv");
        config.setOutput(outFile.getAbsolutePath());
        config.setFailureRate(100.0);

        when(mockStmt.execute(anyString())).thenThrow(new java.sql.SQLException("test error"));

        Main main = buildMockedMain(config);
        main.run();

        String content = new String(java.nio.file.Files.readAllBytes(outFile.toPath()));
        assertTrue(content.contains("false"));
    }

    @Test
    public void testRun_multipleSqls() throws Exception {
        Config config = makeValidConfig();
        File outFile = tempFolder.newFile("multi_out.csv");
        config.setOutput(outFile.getAbsolutePath());
        config.setThreadCount(2);
        config.setRepeatCount(3);
        config.getSqls().put("q2.sql", "SELECT 2;");

        when(mockStmt.execute(anyString())).thenReturn(false);

        Main main = buildMockedMain(config);
        main.run();

        String content = new String(java.nio.file.Files.readAllBytes(outFile.toPath()));
        String[] lines = content.trim().split("\n");
        // header + 3 repeats * 2 sqls = 7 lines
        assertEquals(7, lines.length);
    }

    @Test
    public void testRun_csvHeaderMatchesMetricHeader() throws Exception {
        Config config = makeValidConfig();
        File outFile = tempFolder.newFile("header_out.csv");
        config.setOutput(outFile.getAbsolutePath());

        when(mockStmt.execute(anyString())).thenReturn(false);

        Main main = buildMockedMain(config);
        main.run();

        String content = new String(java.nio.file.Files.readAllBytes(outFile.toPath()));
        String header = content.split("\n")[0];
        assertEquals(Metric.getHeader(), header);
    }

    // ── config detection ────────────────────────────────────────────────────

    @Test
    public void testClickzettaUrlDetection() {
        Config config = new Config();
        config.setJdbcUrl("jdbc:clickzetta://host/db");
        assertTrue(config.getJdbcUrl().startsWith("jdbc:clickzetta://"));
    }

    @Test
    public void testGenericJdbcUrl() {
        Config config = new Config();
        config.setJdbcUrl("jdbc:postgresql://localhost/test");
        assertFalse(config.getJdbcUrl().startsWith("jdbc:clickzetta://"));
    }
}
