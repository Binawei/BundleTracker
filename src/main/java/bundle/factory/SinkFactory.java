package bundle.factory;

import bundle.config.SinkConfiguration;
import bundle.exceptions.FactoryException;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SinkFactory<IN> extends ComponentFactory {
    private static final Logger logger = LoggerFactory.getLogger(SinkFactory.class);
    private final SinkConfiguration configuration;

    protected SinkFactory(SinkConfiguration configuration) {
        this.configuration = configuration;
    }

    public static <IN> SinkFactory<IN> get(SinkConfiguration configuration) throws FactoryException {

        final String className = configuration.getClassName();
        logger.debug("Constructing sink factory with class name '{}'", className);
        Object factory = construct(className, configuration);

        if (!(factory instanceof SinkFactory)) {
            final String message = String.format("Invalid sink factory implementation with type '%s'", factory.getClass().getName());
            logger.error(message);
            throw new FactoryException(message);
        }
        //noinspection unchecked
        return (SinkFactory<IN>) factory;
    }

    public SinkConfiguration getConfiguration() {
        return configuration;
    }

    public abstract SinkFunction<IN> create();
}
