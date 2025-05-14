package bundle.process.enums;

/**
 * Result of sink function invocation.
 * {@link org.apache.flink.streaming.api.functions.sink.SinkFunction::invoke}
 */
public enum SinkResult {
    /**
     * Sink function successfully processed record.
     */
    SUCCESS,

    /**
     * Sink function failed to process record, but did not throw an exception (i.e. non-fatal failure).
     */
    FAILURE
}
