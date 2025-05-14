package bundle.metrics;

import java.io.Serializable;

public interface SinkMetricsHandler extends Serializable {
    void markOut(long n);

    void markOut();

    void markError(long n);

    void markError();
}
