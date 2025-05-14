package bundle.factory;

import bundle.config.SinkConfiguration;
import bundle.sinks.LoggingSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.Serializable;

@SuppressWarnings("unused")
public class LoggingSinkFactory extends SinkFactory<Object> implements Serializable {
    private static final long serialVersionUID = 1L;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    public LoggingSinkFactory(SinkConfiguration configuration) {
        super(configuration);
    }

    @Override
    public SinkFunction<Object> create() {
        return new LoggingSink(getConfiguration());
    }
}
