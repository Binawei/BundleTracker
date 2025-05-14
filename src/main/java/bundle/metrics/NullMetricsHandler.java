package bundle.metrics;

import java.io.Serializable;

/**
 * Null implementation of {@link SinkMetricsHandler}.
 * To be used when metrics are not enabled.
 */
public class NullMetricsHandler implements SinkMetricsHandler, Serializable {
    private static final long serialVersionUID = 1L;

    private static NullMetricsHandler INSTANCE;

    private NullMetricsHandler() {}

    public synchronized static NullMetricsHandler getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new NullMetricsHandler();
        }
        return INSTANCE;
    }

    @Override
    public void markOut(long n) {
        // do nothing
    }

    @Override
    public void markOut() {
        // do nothing
    }

    @Override
    public void markError(long n) {
        // do nothing
    }

    @Override
    public void markError() {
        // do nothing
    }
}
