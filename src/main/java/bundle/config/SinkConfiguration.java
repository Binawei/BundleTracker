package bundle.config;

import bundle.exceptions.ConfigurationException;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SinkConfiguration extends BaseComponentConfiguration implements ComponentConfiguration {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private static final String CONFIG_WRAPPING_ALLOWED_KEY = "wrapping.allowed";
    private final String jobName;

    public SinkConfiguration(Config config, String jobName) throws ConfigurationException {
        super(config);
        this.jobName = jobName;
        if (getConfig().hasPath(CONFIG_MAX_PARALLELISM_KEY)) {
            // setting max parallelism is not supported for sinks
            // a configuration exception is thrown if this value is set to avoid any unexpected issues
            throw new ConfigurationException("Max parallelism is not supported for sinks");
        }
    }

    /**
     * If wrapping is allowed for this sink.
     * This will not explicitly enable wrapping, but will force it to be disabled.
     * Wrapping is only required if the sink does not already provide the correct functionality.
     */
    public boolean shouldAllowWrapping() {
        final boolean wrappingAllowed =  getConfigWrapper().getBoolean(CONFIG_WRAPPING_ALLOWED_KEY, false);
        logger.trace("Allowing wrapping: {}", wrappingAllowed);
        return wrappingAllowed;
    }

    @Override
    public int getMaxParallelism() throws ConfigurationException {
        // setting max parallelism is not supported for sinks
        // a configuration exception is thrown if this value is set to avoid any unexpected issues
        throw new ConfigurationException("Using max parallelism is not supported for sinks");
    }

    public String getJobName() {
        return jobName;
    }
}
