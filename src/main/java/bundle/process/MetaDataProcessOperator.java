package bundle.process;

import bundle.config.ConfigWrapper;
import bundle.config.OperatorConfiguration;
import bundle.exceptions.ConfigurationException;
import bundle.helpers.JsonObjectProcessOperator;
import bundle.helpers.ObjectNodeWrapper;
import bundle.helpers.TemporalHelper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.UUID;

public abstract class MetaDataProcessOperator extends JsonObjectProcessOperator {
    private static final long serialVersionUID = 1L;
    private static final String DEFAULT_META_DATA_KEY = "__meta_data__";
    private static final String CORRELATION_ID = "correlation_id";
    private static final String FIRST_UPDATED = "first_updated";
    private static final String LAST_UPDATED = "last_updated";
    private static final String META_DATA_KEY_KEY = "meta_data_key";

    private final ObjectMapper mapper = new ObjectMapper();
    private final String metaDataKey;

    public MetaDataProcessOperator(OperatorConfiguration.ProcessOperatorConfiguration configuration) throws ConfigurationException {
        super(configuration);

        ConfigWrapper config = new ConfigWrapper(getOperatorConfiguration().getConfig());
        metaDataKey = config.getString(META_DATA_KEY_KEY, DEFAULT_META_DATA_KEY);
    }


    /**
     * Create initial meta data object.
     * @return
     */
    private JsonNode metaData() {
        return mapper.valueToTree(new HashMap<String, Object>() {{
            put(CORRELATION_ID, String.format("%s", UUID.randomUUID().toString()));
            put(FIRST_UPDATED, TemporalHelper.toString(LocalDateTime.now()));
        }});
    }

    /**
     * Create selector path meta data property
     * @param propertyName
     * @return
     */
    private String metaDataPropertyPath(String propertyName) {
        return String.format("/%s/%s", metaDataKey, propertyName);
    }


    protected void processElementInternal(Tuple2<String, ObjectNode> input, ProcessFunction<org.apache.flink.api.java.tuple.Tuple2<java.lang.String, com.fasterxml.jackson.databind.node.ObjectNode>, org.apache.flink.api.java.tuple.Tuple2<java.lang.String, com.fasterxml.jackson.databind.node.ObjectNode>>.Context context, Collector<Tuple2<String, ObjectNode>> collector) throws Exception {
        ObjectNodeWrapper objectNode = new ObjectNodeWrapper(input.f1);

        if (objectNode.select(String.format("/%s", metaDataKey)).isMissingNode()) {
            objectNode.getObjectNode().set(metaDataKey, metaData());
        } else {
            objectNode.put(metaDataPropertyPath(LAST_UPDATED), TemporalHelper.toString(LocalDateTime.now()));
        }

        collector.collect(Tuple2.of(input.f0, objectNode.getObjectNode()));
    }
}
