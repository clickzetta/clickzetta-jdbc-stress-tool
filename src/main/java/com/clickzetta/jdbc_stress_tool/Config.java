package com.clickzetta.jdbc_stress_tool;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Config {

    public static final String CLICKZETTA_DRIVER_CLASS = "com.clickzetta.client.jdbc.ClickZettaDriver";

    public enum ConnectionPoolType {
        HIKARI,
        DBCP,
        DRUID;
    }
    // 枚举类型为字符串
    String jdbcUrl;
    String username;
    String password;
    String initSql = "select 1;";
    int threadCount = 1;
    int repeatCount = 1;
    HashMap<String, String> sqls = new HashMap<>();
    String mode = "seq";
    String output;
    ConnectionPoolType connectionPoolType = ConnectionPoolType.HIKARI;
    String driverClass;
    String prefix;
    double failureRate;

    public void loadFromFile(String configFile) throws IOException {
        FileReader reader = new FileReader(configFile);
        Properties prop = new Properties();
        prop.load(reader);
        jdbcUrl = prop.getProperty("url", prop.getProperty("jdbc"));
        username = prop.getProperty("user", prop.getProperty("username"));
        password = prop.getProperty("password");
        initSql = prop.getProperty("init", "select 1;");
        connectionPoolType = ConnectionPoolType.valueOf(prop.getProperty("poolType", "hikari").toUpperCase());
        driverClass = prop.getProperty("driver");
        if (prop.getProperty("thread") != null) {
            threadCount = Integer.parseInt(prop.getProperty("thread", "1"));
        }
        if (prop.getProperty("repeat") != null) {
            repeatCount = Integer.parseInt(prop.getProperty("repeat", "1"));
        }
        output = prop.getProperty("output");
        prefix = prop.getProperty("prefix", "");
        failureRate = Double.parseDouble(prop.getProperty("failure", "10.0"));
        String sqlPath = prop.getProperty("sql");
        if (sqlPath != null) {
            loadSqlFiles(sqlPath);
        }
        reader.close();
    }

    public void validate() {
        System.out.println("pool    : " + connectionPoolType);
        if (jdbcUrl == null) {
            throw new IllegalArgumentException("jdbc url is null");
        }
        System.out.println("jdbc url: " + jdbcUrl);
        if (jdbcUrl.startsWith("jdbc:clickzetta://")) {
            driverClass = CLICKZETTA_DRIVER_CLASS;
        }
        if (StringUtils.isNotEmpty(driverClass)) {
            System.out.println("driver  : " + driverClass);
        }
        if (username == null) {
            throw new IllegalArgumentException("username is null");
        }
        System.out.println("username: " + username);
        if (password == null) {
            throw new IllegalArgumentException("password is null");
        }
        System.out.println("password: " + StringUtils.repeat('*', password.length()));
        System.out.println("init sql: " + initSql);
        System.out.println("thread  : " + threadCount);
        System.out.println("sql     : " + sqls.keySet().size());
        int sqlCount = 0;
        for (String k : sqls.keySet()) {
            String v = sqls.get(k).trim();
            if (v.length() > 80) {
                System.out.println("  " + k + ": " + v.substring(0, 76) + " ...");
            } else {
                System.out.println("  " + k + ": " + v);
            }
            sqlCount++;
            if (sqlCount > 100) {
                System.out.println("  ...");
                break;
            }
        }
        System.out.println("repeat  : " + repeatCount);
        if (sqls.isEmpty()) {
            throw new IllegalArgumentException("no sql specified");
        }
        System.out.println("total   : " + repeatCount * sqls.keySet().size());
        System.out.println("stop if : fail > " + failureRate + "%");
        if (output == null) {
            throw new IllegalArgumentException("output is null");
        }
        if (prefix != null) {
            Pattern pattern = Pattern.compile("^[0-9a-zA-Z\\-_.]+$");
            Matcher matcher = pattern.matcher(prefix);
            if (!matcher.matches()) {
                prefix = "";
            }
        }
        System.out.println("output  : " + output);
    }

    void loadSqlFiles(String sqlPath) throws IOException {
        ArrayList<File> files = new ArrayList<File>();
        String[] paths = sqlPath.split(",");
        for (String p: paths) {
            File f = new File(p);
            if (f.isFile()) {
                files.add(f);
            } else if (f.isDirectory()) {
                files.addAll(FileUtils.listFiles(f, null, true));
            }
        }
        for (File f : files) {
            String key = f.getName();
            String value = FileUtils.readFileToString(f, "utf-8");
            if (sqls.containsKey(key)) {
                throw new RuntimeException("sql file name must be unique: " + key);
            }
            sqls.put(key, value);
        }
    }
}
