package bundle.templates;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * String lookup function operating on string-keyed JSON records using JSON selectors.
 */
public class JsonRecordLookup extends BaseJsonStringLookup implements TemplateLookup {
    private static final String KEY_SELECTOR = "key";
    private static final String VALUE_SELECTOR = "value";
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Tuple2<String, ObjectNode> record;

    public JsonRecordLookup(Tuple2<String, ObjectNode> record, boolean strict, boolean autoQuote) {
        super(strict, autoQuote);
        if (record == null) {
            final String baseMessage = "Record may not be null";
            logger.error("{}; throwing error", baseMessage);
            throw new RuntimeException(baseMessage);
        }

        this.record = record;
    }

    @Override
    public String lookup(String key) {
        if (key.equalsIgnoreCase(KEY_SELECTOR)) {
            logger.trace("Returning record key for selector '{}'", KEY_SELECTOR);
            return shouldAutoQuote() ? quote(getRecordKey()) : getRecordKey();
        }

        if (key.equalsIgnoreCase(VALUE_SELECTOR)) {
            logger.trace("Returning record value for selector '{}'", VALUE_SELECTOR);
            return getRecordValue().without("__meta_data__").toString();
        }

        return lookup(key, getRecordValue());
    }

    @VisibleForTesting
    public Tuple2<String, ObjectNode> getRecord() {
        return record;
    }

    protected String getRecordKey() {
        return getRecord().f0;
    }

    protected ObjectNode getRecordValue() {
        return getRecord().f1;
    }
}
