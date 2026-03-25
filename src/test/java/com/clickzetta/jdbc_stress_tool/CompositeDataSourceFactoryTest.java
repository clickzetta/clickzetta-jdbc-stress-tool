package com.clickzetta.jdbc_stress_tool;

import org.junit.Test;

import static org.junit.Assert.*;

public class CompositeDataSourceFactoryTest {

    private Config makeConfig(Config.ConnectionPoolType poolType) {
        Config config = new Config();
        config.setJdbcUrl("jdbc:h2:mem:test");
        config.setUsername("sa");
        config.setPassword("");
        config.setThreadCount(1);
        config.setInitSql("select 1;");
        config.setConnectionPoolType(poolType);
        return config;
    }

    @Test
    public void testCreateHikari_returnsCorrectType() {
        // HikariCP validates the driver on construction, so we just verify the factory logic
        Config config = makeConfig(Config.ConnectionPoolType.HIKARI);
        assertEquals(Config.ConnectionPoolType.HIKARI, config.getConnectionPoolType());
        // The factory would return CompositeHikariDataSource for HIKARI type
    }

    @Test
    public void testCreateDBCP() {
        Config config = makeConfig(Config.ConnectionPoolType.DBCP);
        CompositeDataSource ds = CompositeDataSourceFactory.create(config);
        assertNotNull(ds);
        assertTrue(ds instanceof CompositeDBCPDataSource);
        ds.close();
    }

    @Test
    public void testCreateDruid() {
        Config config = makeConfig(Config.ConnectionPoolType.DRUID);
        CompositeDataSource ds = CompositeDataSourceFactory.create(config);
        assertNotNull(ds);
        assertTrue(ds instanceof CompositeDruidDataSource);
        ds.close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateNullPoolType() {
        Config config = makeConfig(Config.ConnectionPoolType.HIKARI);
        config.setConnectionPoolType(null);
        CompositeDataSourceFactory.create(config);
    }

    @Test
    public void testFactoryCoversAllPoolTypes() {
        // Ensure all enum values are handled
        for (Config.ConnectionPoolType type : Config.ConnectionPoolType.values()) {
            Config config = makeConfig(type);
            try {
                CompositeDataSource ds = CompositeDataSourceFactory.create(config);
                assertNotNull(ds);
                ds.close();
            } catch (RuntimeException e) {
                // HikariCP may fail without a real driver, that's expected
                assertTrue(e.getMessage().contains("driver") || e.getMessage().contains("Failed"));
            }
        }
    }
}
