package com.clickzetta.jdbc_stress_tool;

import org.apache.commons.cli.*;
import org.apache.commons.lang3.StringUtils;

import javax.sql.DataSource;
import java.io.*;
import java.sql.Connection;
import java.util.Map;
import java.util.concurrent.*;

public class Main {

    CompositeDataSource cds;
    Config config;
    SqlRunner initSqlRunner;

    Main(Config config) {
        this.config = config;
        this.cds = CompositeDataSourceFactory.create(config);
        if (config.jdbcUrl.startsWith("jdbc:clickzetta://")) {
            this.initSqlRunner = new CZSqlRunner(cds, "init", "select 1;", config.prefix);
        } else {
            this.initSqlRunner = new SqlRunner(cds, "init", "select 1;", config.prefix);
        }
    }

    private void printDot(int i) {
        System.out.print(".");
        if ((i + 1) % 100 == 0) {
            System.out.println();
        }
    }

    private void printX(int i) {
        System.out.print("x");
        if ((i + 1) % 100 == 0) {
            System.out.println();
        }
    }

    boolean validate() {
        // ping
        try {
            // eg. jdbc:postgresql://127.0.0.1:5432/robert
            String host = config.jdbcUrl.split("/")[2].split(":")[0];
            if (config.jdbcUrl.startsWith("jdbc:clickzetta://")) {
                // remove instance part from host
                host = host.split("\\.", 2)[1];
            }
            Runtime rt = Runtime.getRuntime();
            Process p = rt.exec("ping -c 10 " + host);
            BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            while ((line = in.readLine()) != null) {
                System.out.println(line);
            }
            in.close();
        } catch (Throwable e) {
            System.err.println("failed to ping database host, reason " + e.getMessage());
        }
        try {
            // validate jdbc connection &  warm up connection pool with init sql
            Metric metric = initSqlRunner.call();
            System.out.println("validate config done, elapsed " + metric.getClientDuration() + "ms");
            System.out.println("warm up connection pool:");
            long startTimestamp = System.currentTimeMillis();
            DataSource ds = cds.getDataSource();
            Connection[] conns = new Connection[config.threadCount];
            for (int i = 0; i < config.threadCount; i++) {
                conns[i] = ds.getConnection();
                if (StringUtils.isNotEmpty(config.initSql)) {
                    SqlRunner.warmUpConnection(conns[i], config.initSql);
                }
                printDot(i);
            }
            System.out.println();
            System.out.println("active connections: " + cds.getActiveConnections());
            for (int i = 0; i < config.threadCount; i++) {
                conns[i].close();
                printDot(i);
            }
            System.out.println();
            long endTimestamp = System.currentTimeMillis();
            long duration = endTimestamp - startTimestamp;
            System.out.println("done, elapsed " + duration + "ms");
            System.out.println("total  connections: " + cds.getTotalConnections());
            return cds.getTotalConnections() == config.threadCount;
        } catch (Exception e) {
            System.err.println("failed to validate config: " + e.getMessage());
            return false;
        }
    }

    void run() throws IOException {
        System.out.println("running sqls:");
        System.out.printf("[%s] begin ...%n", java.time.LocalDateTime.now());
        BufferedWriter output = new BufferedWriter(new FileWriter(config.output));
        output.write(Metric.getHeader());
        output.write("\n");

        ExecutorService executorService = Executors.newFixedThreadPool(config.threadCount);
        CompletionService completionService = new ExecutorCompletionService(executorService);
        long startTimestamp = System.currentTimeMillis();
        long total = config.repeatCount * config.sqls.size();
        long fail = 0L;
        try {
            for (int i = 0; i < config.repeatCount; i++) {
                for (Map.Entry<String, String> entry : config.sqls.entrySet()) {
                    completionService.submit(initSqlRunner.clone(entry.getKey(), entry.getValue(), config.prefix));
                }
            }
            executorService.shutdown();
            long t = System.currentTimeMillis();
            int c = 0;
            double q = 0;
            for (int i = 0; i < total; i++) {
                Metric metric = (Metric)completionService.take().get();
                output.write(metric.toString());
                output.write("\n");
                if (!metric.isSuccess()) {
                    fail++;
                    // abort test if failure rate is too high
                    if (100.0 * fail / total > config.failureRate) {
                        System.err.println("too many failed sqls, test aborted.");
                        output.close();
                        System.exit(1);
                    }
                }
                if (System.currentTimeMillis() - t >= 10 * 1000) {
                    q = 1000.0 * (i - c) / (System.currentTimeMillis() - t);
                    c = i;
                    t = System.currentTimeMillis();
                    System.out.printf("[%s] %d of %d SQLs executed, %d failed, approx qps %.3f ...%n",
                            java.time.LocalDateTime.now(), i, total, fail, q);
                }
            }
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
            output.close();
            System.exit(1);
        } catch (ExecutionException | IOException e) {
            System.err.println(e.getMessage());
        }
        long endTimestamp = System.currentTimeMillis();
        long duration = endTimestamp - startTimestamp;
        System.out.printf("[%s] done%n", java.time.LocalDateTime.now());
        System.out.println("summary:");
        System.out.println("elapsed: " + duration + "ms");
        System.out.println("sql    : " + total);
        System.out.println("failed : " + fail + " (" + (100.0 * fail / total) + "%)");
        System.out.printf("qps    : %.3f%n", 1.0 * total / duration * 1000);
        output.close();
    }

    public static void main(String[] args) throws IOException {
        Options options = new Options();
        options.addOption(Option.builder()
                        .option("h").longOpt("help")
                        .desc("print help")
                        .hasArg(false).required(false)
                        .build())
                .addOption(Option.builder()
                        .option("t").longOpt("thread")
                        .desc("thread number")
                        .hasArg(true).required(false)
                        .build())
                .addOption(Option.builder()
                        .option("r").longOpt("repeat")
                        .desc("repeat times for each thread")
                        .hasArg(true).required(false)
                        .build())
                .addOption(Option.builder()
                        .option("q").longOpt("sql")
                        .desc("sql files to run in one turn for each thread")
                        .hasArg(true).required(false)
                        .build())
                .addOption(Option.builder()
                        .option("l").longOpt("pool")
                        .desc("connection pool: hikari, dbcp, druid. default hikari")
                        .hasArg(true).required(false)
                        .build())
                .addOption(Option.builder()
                        .option("j").longOpt("jdbc")
                        .desc("jdbc url")
                        .hasArg(true).required(false)
                        .build())
                .addOption(Option.builder()
                        .option("d").longOpt("driver")
                        .desc("jdbc driver class name")
                        .hasArg(true).required(false)
                        .build())
                .addOption(Option.builder()
                        .option("u").longOpt("username")
                        .desc("username for jdbc connection")
                        .hasArg(true).required(false)
                        .build())
                .addOption(Option.builder()
                        .option("p").longOpt("password")
                        .desc("password for jdbc connection")
                        .hasArg(true).required(false)
                        .build())
                .addOption(Option.builder()
                        .option("i").longOpt("init")
                        .desc("init sql for jdbc connection")
                        .hasArg(true).required(false)
                        .build())
                .addOption(Option.builder()
                        .option("c").longOpt("config")
                        .desc("config file, in java properties format")
                        .hasArg(true).required(false)
                        .build())
                .addOption(Option.builder()
                        .option("o").longOpt("output")
                        .desc("output csv file")
                        .hasArg(true).required(false)
                        .build())
                .addOption(Option.builder()
                        .longOpt("prefix")
                        .desc("prefix of job id when running clickzetta sql")
                        .hasArg(true).required(false)
                        .build())
                .addOption(Option.builder()
                        .option("f").longOpt("failure")
                        .desc("test will be aborted if failure rate exceeds this value")
                        .hasArg(true).required(false)
                        .build())
        ;
        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;//not a good practice, it serves it purpose

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("jdbc-stress-tool", options);
            System.exit(1);
        }

        if (cmd.hasOption("help")) {
            formatter.printHelp("jdbc-stress-tool", options);
            System.exit(0);
        }

        Config config = new Config();
        if (cmd.hasOption("config")) {
            // parse config file into context
            config.loadFromFile(cmd.getOptionValue("config"));
        }
        // overwrite context using cmd args
        if (cmd.hasOption("pool")) {
            config.connectionPoolType = Config.ConnectionPoolType.valueOf(cmd.getOptionValue("pool").toUpperCase());
        }
        if (cmd.hasOption("jdbc")) {
            config.jdbcUrl = cmd.getOptionValue("jdbc");
        }
        if (cmd.hasOption("driver")) {
            config.driverClass = cmd.getOptionValue("driver");
        }
        if (cmd.hasOption("username")) {
            config.username = cmd.getOptionValue("username");
        }
        if (cmd.hasOption("password")) {
            config.password = cmd.getOptionValue("password");
        }
        if (cmd.hasOption("init")) {
            config.initSql = cmd.getOptionValue("init");
        }
        if (cmd.hasOption("thread")) {
            config.threadCount = Integer.parseInt(cmd.getOptionValue("thread"));
        }
        if (cmd.hasOption("repeat")) {
            config.repeatCount = Integer.parseInt(cmd.getOptionValue("repeat"));
        }
        if (cmd.hasOption("mode")) {
            config.mode = cmd.getOptionValue("mode");
        }
        if (cmd.hasOption("sql")) {
            config.loadSqlFiles(cmd.getOptionValue("sql"));
        }
        if (cmd.hasOption("output")) {
            config.output = cmd.getOptionValue("output");
        }
        if (cmd.hasOption("prefix")) {
            config.prefix = cmd.getOptionValue("prefix");
        }
        if (cmd.hasOption("failure")) {
            config.failureRate = Double.parseDouble(cmd.getOptionValue("failure"));
        }

        try {
            // validate config and print context
            config.validate();

            Main main = new Main(config);
            // validate jdbc connection && warm up connection pool
            if (!main.validate()) {
                System.exit(1);
            }
            // multi thread run
            main.run();

            System.exit(0);
        } catch (Throwable e) {
            System.err.println(e.getMessage());
        } finally {
            System.exit(1);
        }
    }
}
