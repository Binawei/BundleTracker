package bundle.metrics;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * Implementation of {@link SinkMetricsHandler} recording to standard Flink metrics.
 */
public class SinkMetricsRecorder implements SinkMetricsHandler, Serializable {
    private static final long serialVersionUID = 1L;
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private transient Counter sinkOutCounter, sinkErrorCounter;

    public SinkMetricsRecorder(RuntimeContext runtimeContext) {
        register(runtimeContext);
    }

    protected void register(RuntimeContext runtimeContext) {
        logger.debug("Registering output metrics for sink");
        MetricGroup metricGroup = runtimeContext.getMetricGroup();

        sinkOutCounter = metricGroup.counter("sinkNumRecordsOut");
        metricGroup.meter("sinkNumRecordsOutPerSecond", new MeterView(sinkOutCounter));

        sinkErrorCounter = metricGroup.counter("sinkNumRecordsError");
        metricGroup.meter("sinkNumRecordsErrorPerSecond", new MeterView(sinkErrorCounter));
    }

    @Override
    public void markOut(long n) {
        sinkOutCounter.inc(n);
    }

    @Override
    public void markOut() {
        markOut(1);
    }

    @Override
    public void markError(long n) {
        sinkErrorCounter.inc(n);
    }

    @Override
    public void markError() {
        markError(1);
    }
}
