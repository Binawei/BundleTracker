package bundle.factory;

import bundle.config.OperatorConfiguration;
import bundle.exceptions.FactoryException;
import bundle.process.FilterOperator;
import bundle.process.OperatorFunction;
import bundle.process.ProcessOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class OperatorFactory<S, D> extends ComponentFactory {
    private static final Logger logger = LoggerFactory.getLogger(OperatorFactory.class);
    private final OperatorConfiguration operatorConfiguration;

    protected OperatorFactory(OperatorConfiguration operatorConfiguration) {
        this.operatorConfiguration = operatorConfiguration;
    }

    public abstract OperatorFunction<S, D> create() throws FactoryException;

    public static <S, D> OperatorFactory<S, D> get(OperatorConfiguration operatorConfiguration) {
        logger.debug("Creating operator factory for operation '{}'", operatorConfiguration.getOperation().getCode());

        if (operatorConfiguration instanceof OperatorConfiguration.FilterOperatorConfiguration) {
            OperatorConfiguration.FilterOperatorConfiguration config = (OperatorConfiguration.FilterOperatorConfiguration) operatorConfiguration;
            //noinspection unchecked
            return (OperatorFactory<S, D>) new FilterOperatorFactory<S>(config);
        }

        if (operatorConfiguration instanceof OperatorConfiguration.ProcessOperatorConfiguration) {
            OperatorConfiguration.ProcessOperatorConfiguration config = (OperatorConfiguration.ProcessOperatorConfiguration) operatorConfiguration;
            return new ProcessOperatorFactory<>(config);
        }

        throw new IllegalArgumentException(String.format("Cannot create operator factory for configuration: %s", operatorConfiguration.toString()));
    }

    public OperatorConfiguration getOperatorConfiguration() {
        return operatorConfiguration;
    }

    private static class FilterOperatorFactory<SF> extends OperatorFactory<SF, SF> {
        public FilterOperatorFactory(OperatorConfiguration.FilterOperatorConfiguration operatorConfiguration) {
            super(operatorConfiguration);
        }

        @Override
        public FilterOperator<SF> create() throws FactoryException {
            OperatorConfiguration.FilterOperatorConfiguration configuration = (OperatorConfiguration.FilterOperatorConfiguration) getOperatorConfiguration();
            final String className = configuration.getClassName();
            logger.debug("Constructing filter operator with class name '{}'", className);
            Object operator = construct(className, configuration);
            if (!(operator instanceof FilterOperator)) {
                final String message = String.format("Invalid filter operator implementation with type '%s'", operator.getClass().getName());
                logger.error(message);
                throw new FactoryException(message);
            }
            //noinspection unchecked
            return (FilterOperator<SF>) operator;
        }

    }

    private static class ProcessOperatorFactory<SP, DP> extends OperatorFactory<SP, DP> {
        public ProcessOperatorFactory(OperatorConfiguration.ProcessOperatorConfiguration operatorConfiguration) {
            super(operatorConfiguration);
        }

        @Override
        public ProcessOperator<SP, DP> create() throws FactoryException {
            OperatorConfiguration.ProcessOperatorConfiguration configuration = (OperatorConfiguration.ProcessOperatorConfiguration) getOperatorConfiguration();
            final String className = configuration.getClassName();
            logger.debug("Constructing process operator with class name '{}'", className);
            Object operator = construct(className, configuration);
            if (!(operator instanceof ProcessOperator)) {
                final String message = String.format("Invalid process operator implementation with type '%s'", operator.getClass().getName());
                logger.error(message);
                throw new FactoryException(message);
            }
            //noinspection unchecked
            return (ProcessOperator<SP, DP>) operator;
        }
    }
}
