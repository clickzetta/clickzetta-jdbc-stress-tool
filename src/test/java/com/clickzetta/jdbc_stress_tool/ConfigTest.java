package com.clickzetta.jdbc_stress_tool;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.*;

public class ConfigTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private Config config;

    @Before
    public void setUp() {
        config = new Config();
    }

    // ── defaults ────────────────────────────────────────────────────────────

    @Test
    public void testDefaultValues() {
        assertEquals(1, config.getThreadCount());
        assertEquals(1, config.getRepeatCount());
        assertEquals("select 1;", config.getInitSql());
        assertEquals("seq", config.getMode());
        assertEquals(Config.ConnectionPoolType.HIKARI, config.getConnectionPoolType());
        assertTrue(config.getSqls().isEmpty());
    }

    // ── loadFromFile ────────────────────────────────────────────────────────

    @Test
    public void testLoadFromFile_basic() throws IOException {
        File f = tempFolder.newFile("test.properties");
        try (FileWriter w = new FileWriter(f)) {
            w.write("url=jdbc:postgresql://localhost/test\n");
            w.write("username=user1\n");
            w.write("password=pass1\n");
            w.write("pool=dbcp\n");
            w.write("thread=4\n");
            w.write("repeat=10\n");
            w.write("failure=25.0\n");
            w.write("prefix=myprefix\n");
        }
        config.loadFromFile(f.getAbsolutePath());

        assertEquals("jdbc:postgresql://localhost/test", config.getJdbcUrl());
        assertEquals("user1", config.getUsername());
        assertEquals("pass1", config.getPassword());
        assertEquals(Config.ConnectionPoolType.DBCP, config.getConnectionPoolType());
        assertEquals(4, config.getThreadCount());
        assertEquals(10, config.getRepeatCount());
        assertEquals(25.0, config.getFailureRate(), 0.001);
        assertEquals("myprefix", config.getPrefix());
    }

    @Test
    public void testLoadFromFile_alternateKeys() throws IOException {
        File f = tempFolder.newFile("alt.properties");
        try (FileWriter w = new FileWriter(f)) {
            w.write("jdbc=jdbc:mysql://localhost/db\n");
            w.write("user=u\n");
            w.write("password=p\n");
        }
        config.loadFromFile(f.getAbsolutePath());
        assertEquals("jdbc:mysql://localhost/db", config.getJdbcUrl());
        assertEquals("u", config.getUsername());
    }

    @Test
    public void testLoadFromFile_defaultPoolIsHikari() throws IOException {
        File f = tempFolder.newFile("minimal.properties");
        try (FileWriter w = new FileWriter(f)) {
            w.write("url=jdbc:h2:mem:test\n");
            w.write("username=sa\n");
            w.write("password=\n");
        }
        config.loadFromFile(f.getAbsolutePath());
        assertEquals(Config.ConnectionPoolType.HIKARI, config.getConnectionPoolType());
    }

    @Test
    public void testLoadFromFile_withSqlPath() throws IOException {
        File sqlFile = tempFolder.newFile("q1.sql");
        try (FileWriter w = new FileWriter(sqlFile)) {
            w.write("SELECT 1;");
        }
        File f = tempFolder.newFile("withsql.properties");
        try (FileWriter w = new FileWriter(f)) {
            w.write("url=jdbc:h2:mem:test\n");
            w.write("username=sa\n");
            w.write("password=\n");
            w.write("sql=" + sqlFile.getAbsolutePath() + "\n");
        }
        config.loadFromFile(f.getAbsolutePath());
        assertEquals(1, config.getSqls().size());
        assertTrue(config.getSqls().containsKey("q1.sql"));
    }

    // ── loadSqlFiles ────────────────────────────────────────────────────────

    @Test
    public void testLoadSqlFiles_singleFile() throws IOException {
        File sql = tempFolder.newFile("test.sql");
        try (FileWriter w = new FileWriter(sql)) {
            w.write("SELECT * FROM t;");
        }
        config.loadSqlFiles(sql.getAbsolutePath());
        assertEquals(1, config.getSqls().size());
        assertEquals("SELECT * FROM t;", config.getSqls().get("test.sql"));
    }

    @Test
    public void testLoadSqlFiles_directory() throws IOException {
        File dir = tempFolder.newFolder("sqls");
        for (String name : new String[]{"a.sql", "b.sql"}) {
            File f = new File(dir, name);
            try (FileWriter w = new FileWriter(f)) {
                w.write("SELECT '" + name + "';");
            }
        }
        config.loadSqlFiles(dir.getAbsolutePath());
        assertEquals(2, config.getSqls().size());
    }

    @Test
    public void testLoadSqlFiles_commaSeparated() throws IOException {
        File f1 = tempFolder.newFile("x.sql");
        File f2 = tempFolder.newFile("y.sql");
        try (FileWriter w = new FileWriter(f1)) { w.write("SELECT 1;"); }
        try (FileWriter w = new FileWriter(f2)) { w.write("SELECT 2;"); }
        config.loadSqlFiles(f1.getAbsolutePath() + "," + f2.getAbsolutePath());
        assertEquals(2, config.getSqls().size());
    }

    @Test(expected = RuntimeException.class)
    public void testLoadSqlFiles_duplicateNameThrows() throws IOException {
        File dir1 = tempFolder.newFolder("d1");
        File dir2 = tempFolder.newFolder("d2");
        new FileWriter(new File(dir1, "dup.sql")).close();
        new FileWriter(new File(dir2, "dup.sql")).close();
        // Write content so they're valid
        try (FileWriter w = new FileWriter(new File(dir1, "dup.sql"))) { w.write("a"); }
        try (FileWriter w = new FileWriter(new File(dir2, "dup.sql"))) { w.write("b"); }
        config.loadSqlFiles(dir1.getAbsolutePath() + "," + dir2.getAbsolutePath());
    }

    // ── validate ────────────────────────────────────────────────────────────

    @Test(expected = IllegalArgumentException.class)
    public void testValidate_nullUrl() {
        config.setUsername("u");
        config.setPassword("p");
        config.setOutput("out.csv");
        config.getSqls().put("q", "SELECT 1");
        config.validate();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidate_nullUsername() {
        config.setJdbcUrl("jdbc:h2:mem:test");
        config.setPassword("p");
        config.setOutput("out.csv");
        config.getSqls().put("q", "SELECT 1");
        config.validate();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidate_nullPassword() {
        config.setJdbcUrl("jdbc:h2:mem:test");
        config.setUsername("u");
        config.setOutput("out.csv");
        config.getSqls().put("q", "SELECT 1");
        config.validate();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidate_noSqls() {
        config.setJdbcUrl("jdbc:h2:mem:test");
        config.setUsername("u");
        config.setPassword("p");
        config.setOutput("out.csv");
        config.validate();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidate_nullOutput() {
        config.setJdbcUrl("jdbc:h2:mem:test");
        config.setUsername("u");
        config.setPassword("p");
        config.getSqls().put("q", "SELECT 1");
        config.validate();
    }

    @Test
    public void testValidate_success() {
        config.setJdbcUrl("jdbc:h2:mem:test");
        config.setUsername("u");
        config.setPassword("secret");
        config.setOutput("out.csv");
        config.getSqls().put("q1", "SELECT 1");
        config.validate(); // should not throw
    }

    @Test
    public void testValidate_clickzettaUrlSetsDriver() {
        config.setJdbcUrl("jdbc:clickzetta://host/db");
        config.setUsername("u");
        config.setPassword("p");
        config.setOutput("out.csv");
        config.getSqls().put("q", "SELECT 1");
        config.validate();
        assertEquals(Config.CLICKZETTA_DRIVER_CLASS, config.getDriverClass());
    }

    @Test
    public void testValidate_invalidPrefixCleared() {
        config.setJdbcUrl("jdbc:h2:mem:test");
        config.setUsername("u");
        config.setPassword("p");
        config.setOutput("out.csv");
        config.getSqls().put("q", "SELECT 1");
        config.setPrefix("invalid prefix with spaces!");
        config.validate();
        assertEquals("", config.getPrefix());
    }

    @Test
    public void testValidate_validPrefixKept() {
        config.setJdbcUrl("jdbc:h2:mem:test");
        config.setUsername("u");
        config.setPassword("p");
        config.setOutput("out.csv");
        config.getSqls().put("q", "SELECT 1");
        config.setPrefix("my-prefix_v1.0");
        config.validate();
        assertEquals("my-prefix_v1.0", config.getPrefix());
    }

    // ── ConnectionPoolType enum ─────────────────────────────────────────────

    @Test
    public void testConnectionPoolTypeValues() {
        assertEquals(3, Config.ConnectionPoolType.values().length);
        assertNotNull(Config.ConnectionPoolType.valueOf("HIKARI"));
        assertNotNull(Config.ConnectionPoolType.valueOf("DBCP"));
        assertNotNull(Config.ConnectionPoolType.valueOf("DRUID"));
    }
}
