package bundle.process.rules;

import bundle.config.RuleConfiguration;
import bundle.exceptions.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class BaseRule<T> implements Rule<T> {
    private static final long serialVersionUID = 1L;

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final RuleConfiguration configuration;

    protected BaseRule(RuleConfiguration configuration) {
        this.configuration = configuration;
    }

    public RuleConfiguration getConfiguration() {
        return configuration;
    }

    protected void throwConfigurationException(String message) throws ConfigurationException {
        logger.error(message);
        throw new ConfigurationException(message);
    }
}
