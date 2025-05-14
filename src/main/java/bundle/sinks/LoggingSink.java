package bundle.sinks;

import bundle.config.SinkConfiguration;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;


import java.io.Serializable;

/**
 * Sink function that logs to standard logger.
 */
public class LoggingSink implements SinkFunction<Object>, Serializable {
    private static final long serialVersionUID = 1L;
    private final MessageLoggerFunction<Object> loggerFunction;

    public LoggingSink(SinkConfiguration configuration) {
        loggerFunction = new MessageLoggerFunction<>(getClass(), configuration.getConfig());
    }

    @Override
    public void invoke(Object value, Context context) {
        loggerFunction.log(value);
    }
}
