package bundle.config;


import bundle.process.enums.Operation;
import com.typesafe.config.Config;

public abstract class OperatorConfiguration extends BaseComponentConfiguration implements ComponentConfiguration {
    protected OperatorConfiguration(Config config) {
        super(config);
    }

    public abstract Operation getOperation();

    public static class FilterOperatorConfiguration extends OperatorConfiguration {
        private final FilterErrorHandlingConfiguration errorHandlingConfiguration;

        public FilterOperatorConfiguration(Config config) {
            super(config);
            errorHandlingConfiguration = new FilterErrorHandlingConfiguration(this);
        }

        @Override
        public Operation getOperation() {
            return Operation.FILTER;
        }

        @Override
        public FilterErrorHandlingConfiguration getErrorHandlingConfiguration() {
            return errorHandlingConfiguration;
        }
    }

    public static class ProcessOperatorConfiguration extends OperatorConfiguration {
        public ProcessOperatorConfiguration(Config config) {
            super(config);
        }

        @Override
        public Operation getOperation() {
            return Operation.PROCESS;
        }
    }
}
