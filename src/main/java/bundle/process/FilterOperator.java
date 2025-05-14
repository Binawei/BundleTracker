package bundle.process;

import bundle.config.FilterErrorHandlingConfiguration;
import bundle.config.OperatorConfiguration;
import org.apache.flink.api.common.functions.FilterFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base for filter operators that do not perform any transformations.
 * @param <S> source type
 */
public abstract class FilterOperator<S> implements OperatorFunction<S, S>, FilterFunction<S> {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final OperatorConfiguration.FilterOperatorConfiguration configuration;

    protected FilterOperator(OperatorConfiguration.FilterOperatorConfiguration configuration) {
        this.configuration = configuration;
    }

    protected abstract boolean filterInternal(S value) throws Exception;

    @Override
    public boolean filter(S value) throws Exception {
        try {
            return filterInternal(value);
        } catch(Exception exception) {
            logger.error("Error filtering value", exception);
            final FilterErrorHandlingConfiguration errorHandlingConfiguration = getOperatorConfiguration().getErrorHandlingConfiguration();
            if (!errorHandlingConfiguration.shouldIgnoreErrors()) {
                throw exception;
            }
            final boolean shouldDiscardError = errorHandlingConfiguration.shouldDiscardErrors();
            logger.warn("Discarding erroneous record: {}", shouldDiscardError);
            return !shouldDiscardError;
        }
    }

    public OperatorConfiguration.FilterOperatorConfiguration getOperatorConfiguration() {
        return configuration;
    }
}
