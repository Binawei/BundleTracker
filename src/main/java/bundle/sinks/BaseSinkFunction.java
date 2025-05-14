package bundle.sinks;

import bundle.config.ErrorHandlingConfiguration;
import bundle.config.SinkConfiguration;
import bundle.metrics.NullMetricsHandler;
import bundle.metrics.SinkMetricsHandler;
import bundle.metrics.SinkMetricsRecorder;
import bundle.process.enums.SinkResult;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;


import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static net.logstash.logback.marker.Markers.appendEntries;

/**
 * Base sink function.
 * Provides output metrics recording.
 * @param <IN> type of input value to sink
 */
public abstract class BaseSinkFunction<IN> extends RichSinkFunction<IN> implements SinkFunction<IN>, Serializable {
    private static final long serialVersionUID = 1L;
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final SinkConfiguration configuration;
    private final boolean outputMetricsEnabled;

    // no sink metrics until the sink function has been opened
    private transient SinkMetricsHandler metricsHandler = NullMetricsHandler.getInstance();

    protected BaseSinkFunction(SinkConfiguration configuration, boolean outputMetricsEnabled) {
        this.configuration = configuration;
        this.outputMetricsEnabled = outputMetricsEnabled;
    }

    protected BaseSinkFunction(SinkConfiguration configuration) {
        this(configuration, true);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        if (areOutputMetricsEnabled()) {
            metricsHandler = new SinkMetricsRecorder(getRuntimeContext());
        }
    }

    public SinkConfiguration getConfiguration() {
        return configuration;
    }

    /**
     * Invoke internal sink function, returning a result value.
     * Returning success will log an output write operation to the metrics handler.
     * Returning failure or throwing an exception will log a failure to the metrics handler.
     * @return result of invocation - determines logged metric type
     */
    protected abstract SinkResult invokeInternal(IN value, @SuppressWarnings("rawtypes") Context context) throws Exception;

    @Override
    public void invoke(IN value, Context context) throws Exception {
        final SinkResult result;
        try {
            result = invokeInternal(value, context);
            logger.trace("Sink result: {}", result);
        } catch (Exception exception) {
            logger.error("Error invoking internal sink function", exception);
            metricsHandler.markError(); // mark as failure

            final ErrorHandlingConfiguration errorHandlingConfiguration = getConfiguration().getErrorHandlingConfiguration();

            if (errorHandlingConfiguration.shouldLogErrorRecords()) {
                final RuntimeContext runtimeContext = getRuntimeContext();
                final Marker valueMarker = toLogMarker(value);
                final Map<String, Object> map = new HashMap<String, Object>() {{
                    put("job_name", getConfiguration().getJobName());
                    put("operator_name", getConfiguration().getName());
                    put("task_name", runtimeContext.getTaskName());
                    put("subtask_index", runtimeContext.getIndexOfThisSubtask());
                    put("ignored", getConfiguration().getErrorHandlingConfiguration().shouldIgnoreErrors());
                }};
                if (errorHandlingConfiguration.shouldLogErrorRecordsException()) {
                    map.put("exception_class", exception.getClass().getName());
                    map.put("exception_message", exception.getMessage());
                }
                Marker marker = appendEntries(map).and(valueMarker);
                logger.error(marker, "[ERROR:SINK:RECORD]");
            }

            if (errorHandlingConfiguration.shouldIgnoreErrors()) {
                logger.warn("Ignoring error for sink operation");
                return; // must return in order to not log error twice
            }

            throw exception;
        }

        if (areOutputMetricsEnabled()) {
            switch (result) {
                case SUCCESS:
                    metricsHandler.markOut();
                    break;
                case FAILURE:
                    metricsHandler.markError();
                    break;
                default:
                    // sanity-check
                    final String errorMessage = String.format("Unsupported sink result: %s", result);
                    logger.error(errorMessage);
                    throw new RuntimeException(errorMessage);
            }
        }
    }

    protected boolean areOutputMetricsEnabled() {
        return outputMetricsEnabled;
    }

    /**
     * Convert an input value into a marker for logging.
     */
    protected abstract Marker toLogMarker(IN value);
}
