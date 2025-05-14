package bundle.config;

import bundle.exceptions.ConfigurationException;
import bundle.helpers.ConfigurationHelper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public abstract class BaseComponentConfiguration implements ComponentConfiguration {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private static final String CONFIG_PROPERTIES_KEY = "properties";
    private static final String CONFIG_NAME_KEY = "name";
    private static final String CONFIG_CLASS_NAME_KEY = "class";
    protected static final String CONFIG_PARALLELISM_KEY = "parallelism.value";
    protected static final String CONFIG_MAX_PARALLELISM_KEY = "parallelism.max";

    private final Config config;
    private final ConfigWrapper configWrapper;
    private final Properties properties;
    private final ErrorHandlingConfiguration errorHandlingConfiguration;

    protected BaseComponentConfiguration(Config config) {
        this.config = config;
        configWrapper = new ConfigWrapper(config);

        if (config.hasPath(CONFIG_PROPERTIES_KEY)) {
            properties = ConfigurationHelper.toProperties(config.getConfig(CONFIG_PROPERTIES_KEY));
        } else {
            properties = null;
        }

        errorHandlingConfiguration = new ErrorHandlingConfiguration(this);
    }

    @Override
    public String getName() {
        return getConfig().getString(CONFIG_NAME_KEY);
    }

    @Override
    public String getClassName() {
        return getConfig().getString(CONFIG_CLASS_NAME_KEY);
    }

    @Override
    public Config getConfig() {
        return config;
    }

    public ConfigWrapper getConfigWrapper() {
        return configWrapper;
    }

    @Override
    public Properties getProperties() {
        return properties;
    }



    @Override
    public int getParallelism() throws ConfigurationException {
        return getParallelismValue(CONFIG_PARALLELISM_KEY);
    }

    @Override
    public int getMaxParallelism() throws ConfigurationException {
        return getParallelismValue(CONFIG_MAX_PARALLELISM_KEY);
    }

    /**
     * Helper method to validate and get a parallelism value.
     * @param key parallelism value key
     */
    private int getParallelismValue(String key) throws ConfigurationException {
        if (!getConfig().hasPath(key)) {
            return 0; // default unset value
        }
        int parallelism;
        try {
            parallelism = getConfig().getInt(key);
        } catch (ConfigException.WrongType wrongTypeException) {
            // note: this exception will not be thrown if the value can be parsed to an integer, for example "1"
            final String errorMessage = String.format("Invalid type for parallelism value for key '%s'", key);
            logger.error(errorMessage, wrongTypeException);
            throw new ConfigurationException(errorMessage, wrongTypeException);
        }
        if (parallelism < 0) {
            // parallelism value must not be negative - configuration error
            final String errorMessage = String.format("Invalid negative parallelism value for key '%s': %d", key, parallelism);
            logger.error(errorMessage);
            throw new ConfigurationException(errorMessage);
        }
        return parallelism;
    }

    public ErrorHandlingConfiguration getErrorHandlingConfiguration() {
        return errorHandlingConfiguration;
    }
}
