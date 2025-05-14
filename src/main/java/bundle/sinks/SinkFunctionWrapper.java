package bundle.sinks;

import bundle.config.ErrorHandlingConfiguration;
import bundle.config.SinkConfiguration;
import bundle.metrics.NullMetricsHandler;
import bundle.metrics.SinkMetricsHandler;
import bundle.metrics.SinkMetricsRecorder;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * Wrapper to generic sink functions.
 * @param <IN> type of input value to sink function
 */
public class SinkFunctionWrapper<IN> extends RichSinkFunction<IN> implements SinkFunction<IN>, Serializable {
    private static final long serialVersionUID = 1L;
    private static final String OUTPUT_METRICS_ENABLED_KEY = "metrics.output.enabled";
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final SinkFunction<IN> wrappedSinkFunction;
//    private final boolean outputMetricsEnabled;
    private final SinkConfiguration configuration;
    private transient SinkMetricsHandler metricsHandler;

    public SinkFunctionWrapper(SinkConfiguration configuration, SinkFunction<IN> sinkFunction) {
        super();
        if (configuration == null) {
            throw new IllegalArgumentException("Sink configuration may not be null");
        }
        this.configuration = configuration;
        if (sinkFunction == null) {
            throw new IllegalArgumentException("Sink function to wrap may not be null");
        }
        wrappedSinkFunction = sinkFunction;

        // output metrics enabled by default unless explicitly disabled
        logger.trace("Getting output metrics enabled state from configuration key '{}' (defaulting to true)", OUTPUT_METRICS_ENABLED_KEY);
//        outputMetricsEnabled = getConfiguration().getBoolean(OUTPUT_METRICS_ENABLED_KEY, true);

//        logger.info("Wrapping sink function '{}' with output metric recording {}",
//                getWrappedSinkFunction().getClass().getName(), areOutputMetricsEnabled() ? "enabled" : "disabled");
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        if (getWrappedSinkFunction() instanceof RichFunction) {
            ((RichFunction)getWrappedSinkFunction()).open(parameters);
        }
//        metricsHandler = areOutputMetricsEnabled() ? new SinkMetricsRecorder(getRuntimeContext()) : NullMetricsHandler.getInstance();
    }

    @Override
    public void setRuntimeContext(RuntimeContext runtimeContext) {
        super.setRuntimeContext(runtimeContext);
        if (getWrappedSinkFunction() instanceof RichFunction) {
            ((RichFunction)getWrappedSinkFunction()).setRuntimeContext(runtimeContext);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (getWrappedSinkFunction() instanceof RichFunction) {
            ((RichFunction)getWrappedSinkFunction()).close();
        }
    }

    @Override
    public void invoke(IN value, Context context) throws Exception {
        try {
            getWrappedSinkFunction().invoke(value, context);
            logger.debug("No error invoking wrapped sink function; marking as output");
            metricsHandler.markOut(); // assume success if no exception is thrown
        } catch(Exception exception) {
            logger.error("Error invoking wrapped sink function; marking as error");
            metricsHandler.markError();
            final ErrorHandlingConfiguration errorHandlingConfiguration = getConfiguration().getErrorHandlingConfiguration();
            if (errorHandlingConfiguration.shouldIgnoreErrors()) {
                logger.warn("Ignoring error for wrapped sink operation");
                return; // must return in order to not log error twice
            }
            throw exception;
        }
    }

    @VisibleForTesting
    protected SinkFunction<IN> getWrappedSinkFunction() {
        return wrappedSinkFunction;
    }

//    @VisibleForTesting
//    protected boolean areOutputMetricsEnabled() {
//        return outputMetricsEnabled;
//    }

    @VisibleForTesting
    protected SinkConfiguration getConfiguration() {
        return configuration;
    }
}
