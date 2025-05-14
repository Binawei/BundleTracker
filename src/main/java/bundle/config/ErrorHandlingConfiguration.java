package bundle.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Serializable;

public class ErrorHandlingConfiguration implements Serializable {
    private static final long serialVersionUID = 1L;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final ConfigWrapper configWrapper;

    private static final String CONFIG_ERROR_ROOT_KEY = "errors";
    private static final String CONFIG_IGNORE_ERRORS_KEY = "ignore";
    private static final String CONFIG_COLLECT_ERROR_RECORDS = "collect";
    private static final String CONFIG_LOG_ERROR_RECORDS_KEY = "log.record";
    private static final String CONFIG_LOG_ERROR_RECORDS_EXCEPTION_KEY = "log.exception";

    public ErrorHandlingConfiguration(BaseComponentConfiguration componentConfiguration) {
        this(componentConfiguration.getConfig().hasPath(CONFIG_ERROR_ROOT_KEY) ? componentConfiguration.getConfig().getConfig(CONFIG_ERROR_ROOT_KEY) : null);
    }

    public ErrorHandlingConfiguration(@Nullable Config errorConfig) {
        if (errorConfig == null) {
            logger.info("No error configuration in config object; assuming empty config");
            configWrapper = new ConfigWrapper(ConfigFactory.empty());
        } else {
            configWrapper = new ConfigWrapper(errorConfig);
        }
    }

    /**
     * If records that fail to process should be collected, along with the errors that caused them.
     * Defaults to false.
     */
    public boolean shouldCollectErrorRecords() {
        final boolean collectErrorRecords =  getConfigWrapper().getBoolean(CONFIG_COLLECT_ERROR_RECORDS, false);
        logger.trace("Collecting error records: {}", collectErrorRecords);
        return collectErrorRecords;
    }

    /**
     * If errors should be ignored (i.e. logged and suppressed) or an exception thrown.
     * Defaults to false (i.e. failing on errors).
     */
    public boolean shouldIgnoreErrors() {
        final boolean ignoreErrors =  getConfigWrapper().getBoolean(CONFIG_IGNORE_ERRORS_KEY, false);
        logger.trace("Ignoring errors: {}", ignoreErrors);
        return ignoreErrors;
    }

    /**
     * If records that fail to process should be logged.
     * This will log the full value for records that result in a processing error.
     * Defaults to false.
     */
    public boolean shouldLogErrorRecords() {
        final boolean logErrorRecords =  getConfigWrapper().getBoolean(CONFIG_LOG_ERROR_RECORDS_KEY, false);
        logger.trace("Logging error records: {}", logErrorRecords);
        return logErrorRecords;
    }

    /**
     * If the exception should also be logged along with error records.
     */
    public boolean shouldLogErrorRecordsException() {
        final boolean logErrorRecordsException =  getConfigWrapper().getBoolean(CONFIG_LOG_ERROR_RECORDS_EXCEPTION_KEY, false);
        logger.trace("Logging error records exception: {}", logErrorRecordsException);
        return logErrorRecordsException;    }

    protected ConfigWrapper getConfigWrapper() {
        return configWrapper;
    }
}
