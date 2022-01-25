package net.iponweb.disthene.remover;

import org.apache.commons.cli.*;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.builder.api.*;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicLong;

public class DistheneRemover {

    private static Logger logger;

    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_RED = "\u001B[31m";

    public static void main(String[] args) throws IOException {
        configureLog();

        Options options = new Options();

        OptionGroup operationGroup = new OptionGroup();
        operationGroup.addOption(new Option("rt", "remove-tenant", false, "Irrevocably remove tenant"));
        operationGroup.addOption(new Option("dm", "delete-metrics", false, "Delete metrics by wildcard"));
        operationGroup.addOption(new Option("h", "help", false, "Usage help"));
        operationGroup.setRequired(true);

        options.addOptionGroup(operationGroup);

        // common options
        options.addOption("c", "cassandra", true, "Cassandra contact point");
        options.addOption("e", "elastic", true, "Elasticsearch contact point");
        options.addOption("t", "tenant", true, "Tenant");

        // delete metrics options
        options.addOption("w", "wildcard", true, "Metrics to delete wildcard");
        options.addOption("dr", "dry-run", false, "Only output metrics to delete");

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine commandLine = parser.parse(options, args);

            switch (operationGroup.getSelected()) {
                case "h": {
                    printHelp(options);
                    break;
                }
                case "rt": {
                    removeTenant(commandLine);
                    break;
                }
                case "dm": {
                    deleteMetrics(commandLine);
                    break;
                }
            }

        } catch (ParseException | MissingOptionsException e) {
            System.out.println(e.getMessage());
            printHelp(options);
            System.exit(1);
        }

        System.exit(0);
    }

    private static void deleteMetrics(CommandLine commandLine) throws IOException, MissingOptionsException {
        if (!commandLine.hasOption("c") || !commandLine.hasOption("e") || !commandLine.hasOption("w") || !commandLine.hasOption("t")) {
            System.out.println("One or more of the required [c, e, w, t] options are not specified");
            throw new MissingOptionsException("One or more of the required [c, e, w, t] options are not specified");
        }

        IndexService indexService = new IndexService(commandLine.getOptionValue("e"), commandLine.getOptionValue("t"));

        AtomicLong counter = new AtomicLong();

        if (commandLine.hasOption("dr")) {
            logger.info("Will delete the following metrics:");
            indexService.process(commandLine.getOptionValue("w"), metric -> {
                counter.getAndIncrement();
                System.out.println(metric);
            });

            logger.info("Found " + counter.get() + " metrics");
        } else {
            Scanner sc= new Scanner(System.in);
            System.out.print(ANSI_RED + "You are about to irrevocably delete metrics with wildcard \"" + commandLine.getOptionValue("w") + "\" from tenant " + commandLine.getOptionValue("t") + ". Are you sure? (Expected answer: \"Yes, I am\"): " + ANSI_RESET);
            String answer = sc.nextLine();

            if (!"Yes, I am".equals(answer)) {
                System.out.println("Operation cancelled");
                return;
            }

            CassandraService cassandraService = new CassandraService(commandLine.getOptionValue("c"), commandLine.getOptionValue("t"));

            indexService.process(commandLine.getOptionValue("w"), metric -> {
                cassandraService.deleteMetric(metric);
                counter.getAndIncrement();
            });
            indexService.deleteByWildCard(commandLine.getOptionValue("w"));

            cassandraService.close();

            logger.info("Deleted " + counter.get() + " metrics");
        }

        indexService.close();

        logger.info("All done");
    }

    private static void removeTenant(CommandLine commandLine) throws IOException, MissingOptionsException {
        if (!commandLine.hasOption("c") || !commandLine.hasOption("e") || !commandLine.hasOption("t")) {
            System.out.println("One or more of the required [c, e, t] options are not specified");
            throw new MissingOptionsException("One or more of the required [c, e, t] options are not specified");
        }

        Scanner sc= new Scanner(System.in);
        System.out.print(ANSI_RED + "You are about to irrevocably remove tenant \"" + commandLine.getOptionValue("t") + "\". Are you sure? (Expected answer: \"Yes, I am sure I want to remove " + commandLine.getOptionValue("t") + "\"): " + ANSI_RESET);
        String answer = sc.nextLine();

        if (!("Yes, I am sure I want to remove " + commandLine.getOptionValue("t")).equals(answer)) {
            System.out.println("Operation cancelled");
            return;
        }

        IndexService indexService = new IndexService(commandLine.getOptionValue("e"), commandLine.getOptionValue("t"));
        CassandraService cassandraService = new CassandraService(commandLine.getOptionValue("c"), commandLine.getOptionValue("t"));

        indexService.removeTenant();
        cassandraService.truncateTenantTables();

        logger.info("All done");
    }

    private static void printHelp(Options options) {
        new HelpFormatter().printHelp("disthene-remover", options);
    }

    private static void configureLog() {
        Level logLevel = Level.INFO;

        ConfigurationBuilder<BuiltConfiguration> builder = ConfigurationBuilderFactory.newConfigurationBuilder();

        RootLoggerComponentBuilder rootLogger = builder.newRootLogger(logLevel);

        // console
        LayoutComponentBuilder layout = builder.newLayout("PatternLayout")
                .addAttribute("pattern", "%p %d{dd.MM.yyyy HH:mm:ss,SSS} [%t] %c %x - %m%n");

        AppenderComponentBuilder console = builder.newAppender("stdout", "Console").add(layout);
        builder.add(console);
        rootLogger.add(builder.newAppenderRef("stdout"));

        builder.add(rootLogger);

        Configurator.initialize(builder.build());

        logger = LogManager.getLogger(DistheneRemover.class);
    }
}
