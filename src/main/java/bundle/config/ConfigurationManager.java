package bundle.config;

import bundle.exceptions.ConfigurationException;
import bundle.process.MetaDataProcessOperator;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.cli.*;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.ReadableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Manager for streaming application configuration.
 */
public class ConfigurationManager {
    private static final String ROOT_CONFIG_ELEMENT_NAME = "application";
    private static final String IS_LOCAL_KEY = "local";
    private static final String SOURCE_KEY = "source";
    private static final String TRACE_ENABLED_KEY = "trace.enabled";
    private static final String TRACE_META_DATA_KEY = "trace.meta_data_key";
    private static final String DEFAULT_TRACE_META_DATA_KEY = "__meta_data__";
    private static final String OPERATORS_KEY = "operators";
    private static final String SINK_KEY = "sink";
    private static final String FILTER_OPERATION = "filter";
    private static final String PROCESS_OPERATION = "process";
    private static final String ENVIRONMENT_KEY = "environment";
    private static final Logger logger = LoggerFactory.getLogger(ConfigurationManager.class);

    private static ConfigurationManager INSTANCE;

    private final Config config;


    private ConfigurationManager(File configFile) throws FileNotFoundException {
        if (!configFile.exists() || !configFile.canRead()) {
            throw new FileNotFoundException(String.format("Cannot read configuration file %s", configFile.getPath()));
        }

        logger.info("Loading configuration file: {}", configFile.getPath());
        config = ConfigFactory.parseFile(configFile).resolve();
    }

    public static ConfigurationManager getInstance() {
        return INSTANCE;
    }
    /**
     * Indicates if the application is running in a local stream environment.
     * Defaults to true if the field is not set in the configuration file.
     * @return true if running in a local stream environment
     */
    public boolean getIsLocal() {
        Config config = getApplicationConfig();
        return !config.hasPath(IS_LOCAL_KEY) || config.getBoolean(IS_LOCAL_KEY);
    }

    public Config getApplicationConfig() {
        return config.getConfig(ROOT_CONFIG_ELEMENT_NAME);
    }

    public synchronized static ConfigurationManager load(String[] args) throws IllegalArgumentException, FileNotFoundException {
        if (INSTANCE == null) {
            final String configFileOption = "file";
            CommandLineParser parser = new DefaultParser();
            Options options = new Options();
            options.addOption("f", configFileOption, true, "Path to configuration file to load");
            CommandLine cmd;
            try {
                cmd = parser.parse(options, args);
            } catch (ParseException e) {
                final String message = "Error parsing configuration";
                logger.error(message, e);
                throw new RuntimeException(message, e);
            }
            String configPath = cmd.getOptionValue(configFileOption);
            if (configPath == null || configPath.length() == 0) {
                throw new IllegalArgumentException("Configuration file path must be provided as a commandline argument");
            }

            final File configFile = new File(configPath);
            INSTANCE = load(configFile);
        }
        return INSTANCE;
    }
    /**
     * Get stream environment configuration.
     */
    public Configuration getEnvironmentConfiguration() {
        final Optional<File> configurationDirectory = getEnvironmentConfigurationDirectory();
        return configurationDirectory.map(dir -> {
            logger.info("Loading environment configuration from path: {}", dir.getAbsolutePath());
            return GlobalConfiguration.loadConfiguration(dir.getAbsolutePath());
        }).orElseGet(() -> {
            logger.info("Environment configuration directory does not exist; creating empty configuration");
            return new Configuration();
        });
    }

    /**
     * Get path to directory with stream environment configuration, if it exists.
     * Empty if the configuration directory does not exist.
     */
    private Optional<File> getEnvironmentConfigurationDirectory() {
        final File current = new File(System.getProperty("user.dir"));

        List<File> candidates = Stream.of(
                Optional.ofNullable(System.getenv(ConfigConstants.ENV_FLINK_CONF_DIR))
                        .map(File::new)
                        .orElse(null),
                new File(current, "conf"),
                new File(current, "config")
        ).filter(Objects::nonNull)
                .distinct()
                .collect(Collectors.toList());
        candidates.forEach(file -> logger.info("Candidate environment configuration directory: {}", file));

        return candidates.stream()
                .filter(Objects::nonNull)
                .filter(candidate -> new File(candidate, GlobalConfiguration.FLINK_CONF_FILENAME).exists())
                .findFirst();
    }

    public String getJobName() {
        return getApplicationConfig().getString("name");
    }

    public SourceConfiguration getSourceConfiguration() {
        logger.trace("Parsing source configuration");
        return new SourceConfiguration(getApplicationConfig().getConfig(SOURCE_KEY));
    }

    public String getTraceMetaDataKey() {
        ConfigWrapper config = new ConfigWrapper(getApplicationConfig());

        return config.getString(TRACE_META_DATA_KEY, DEFAULT_TRACE_META_DATA_KEY);
    }

    public boolean traceEnabled() {
        ConfigWrapper config = new ConfigWrapper(getApplicationConfig());

        return config.getBoolean(TRACE_ENABLED_KEY, true);
    }

    private OperatorConfiguration traceOperatorConfiguration(String name) {
        Config config = getApplicationConfig();

        return new OperatorConfiguration.ProcessOperatorConfiguration(
                ConfigFactory.parseProperties(
                        new Properties() {{
                            put("class", MetaDataProcessOperator.class.getCanonicalName());
                            put("operation", "process");
                            put("name", name);
                            if (config.hasPath(TRACE_META_DATA_KEY)) {
                                put("meta_data_key", config.getString(TRACE_META_DATA_KEY));
                            }
                        }}
                )
        );
    }

    public List<OperatorConfiguration> getOperatorConfigurations() {
        ConfigWrapper config = new ConfigWrapper(getApplicationConfig());
        if (!config.hasPath(OPERATORS_KEY)) {
            return new ArrayList<>();
        }
        List<? extends Config> configs = config.getConfigList(OPERATORS_KEY, Collections.emptyList());
        List<OperatorConfiguration> operatorConfigurations = configs.stream().map(this::createOperatorConfiguration).collect(Collectors.toList());

        if(config.getBoolean(TRACE_ENABLED_KEY, true)) {
            // Insert trace operator at start of operator chain
            operatorConfigurations.add(0, traceOperatorConfiguration("__post_source_hook__"));
            // Append trace operator to end of operator chain
            operatorConfigurations.add(traceOperatorConfiguration("__pre_sink_hook__"));
        }

        return operatorConfigurations;
    }

    public SinkConfiguration getSinkConfiguration() throws ConfigurationException {
        logger.trace("Parsing sink configuration");
        return new SinkConfiguration(getApplicationConfig().getConfig(SINK_KEY), getJobName());
    }

    public ReadableConfig getEnvConfiguration() {
        logger.trace("Parsing Flink environment configuration");
        return FlinkConfigurationWrapper.of(getApplicationConfig().getConfig(ENVIRONMENT_KEY));
    }

    public boolean hasEnvConfig() {
        return getApplicationConfig().hasPath(ENVIRONMENT_KEY);
    }

    private OperatorConfiguration createOperatorConfiguration(Config operatorConfig) {
        final String operation = operatorConfig.getString("operation");
        switch (operation) {
            case FILTER_OPERATION:
                return new OperatorConfiguration.FilterOperatorConfiguration(operatorConfig);
            case PROCESS_OPERATION:
                return new OperatorConfiguration.ProcessOperatorConfiguration(operatorConfig);
            default:
                throw new RuntimeException(String.format("Cannot create configuration for operation '%s'", operation));
        }
    }

    protected static void clear() {
        INSTANCE = null;
    }

    private static ConfigurationManager load(File configFile) throws FileNotFoundException {
        return new ConfigurationManager(configFile);
    }
}
