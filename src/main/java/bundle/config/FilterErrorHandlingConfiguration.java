package bundle.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class FilterErrorHandlingConfiguration extends ErrorHandlingConfiguration implements Serializable {
    private static final long serialVersionUID = 1L;
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private static final String CONFIG_DISCARD_ERROR_KEY = "discard";

    public FilterErrorHandlingConfiguration(OperatorConfiguration.FilterOperatorConfiguration filterConfiguration) {
        super(filterConfiguration);
    }

    /**
     *
     * If errors should be discarded (true) or retained (false).
     * Default to discarding errors.
     */
    public boolean shouldDiscardErrors() {
        final boolean shouldDiscard = getConfigWrapper().getBoolean(CONFIG_DISCARD_ERROR_KEY, true);
        logger.trace("Discarding error for filter operation: {}", shouldDiscard);
        return shouldDiscard;
    }
}
