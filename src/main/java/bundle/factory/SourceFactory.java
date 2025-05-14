package bundle.factory;

import bundle.config.SourceConfiguration;
import bundle.exceptions.ConfigurationException;
import bundle.exceptions.FactoryException;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class SourceFactory<OUT> extends ComponentFactory {
    private static final Logger logger = LoggerFactory.getLogger(SourceFactory.class);
    private final SourceConfiguration configuration;

    protected SourceFactory(SourceConfiguration configuration) {
        this.configuration = configuration;
    }

    public static <OUT> SourceFactory<OUT> get(SourceConfiguration configuration) throws FactoryException {
        final String className = configuration.getClassName();
        logger.debug("Constructing source factory with class name '{}'", className);
        Object factory = construct(className, configuration);

        if (!(factory instanceof SourceFactory)) {
            final String message = String.format("Invalid source factory implementation with type '%s'", factory.getClass().getName());
            logger.error(message);
            throw new FactoryException(message);
        }
        //noinspection unchecked
        return (SourceFactory<OUT>) factory;
    }

    public SourceConfiguration getConfiguration() {
        return configuration;
    }

    public abstract SourceFunction<OUT> create() throws ConfigurationException;

    /**
     * Type hint for output of source.
     */
    public abstract TypeHint<OUT> returns();
}
