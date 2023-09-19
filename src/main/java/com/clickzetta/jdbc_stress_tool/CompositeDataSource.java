package com.clickzetta.jdbc_stress_tool;

import com.clickzetta.client.jdbc.core.CZStatement;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.sql.Statement;

public interface CompositeDataSource {

    DataSource getDataSource();

    int getActiveConnections();

    int getTotalConnections();

    CZStatement castToCZStatement(Statement statement) throws SQLException;
}
