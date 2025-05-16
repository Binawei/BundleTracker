package bundle.source.enrichment;

import bundle.exceptions.ConfigurationException;
import bundle.helpers.ObjectNodeWrapper;
import bundle.process.JsonSelector;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Serializable;

public class KafkaSourceEnrichment implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(KafkaSourceEnrichment.class);

    final static String TOPIC_SELECTOR_PATH = "topic.selector";

    private final JsonSelector topicSelector;

    private KafkaSourceEnrichment(@Nullable JsonSelector topicSelector) {
        this.topicSelector = topicSelector;
    }

    public static KafkaSourceEnrichment of(@Nullable Config enrichmentConfig) throws ConfigurationException {

        if (enrichmentConfig == null) {
            return new NoOperationKafkaSourceEnrichment();
        }

        if (enrichmentConfig.hasPath(TOPIC_SELECTOR_PATH)) {
            final String selector = enrichmentConfig.getString(TOPIC_SELECTOR_PATH);
            return new KafkaSourceEnrichment(JsonSelector.parse(selector));
        } else {
            final String msg = String.format("Only topic enrichment allowed presently. Must define the {} field in config", TOPIC_SELECTOR_PATH);
            throw new ConfigurationException(msg);
        }
    }

    public ObjectNode enrich(ObjectNode value, String topic) {
        if (topicSelector != null) {
            final ObjectNodeWrapper wrapper = new ObjectNodeWrapper(value);
            wrapper.put(topicSelector, topic);
        }
        return value;
    }

    private static class NoOperationKafkaSourceEnrichment extends KafkaSourceEnrichment {
        public NoOperationKafkaSourceEnrichment() {
            super(null);
        }

        @Override
        public ObjectNode enrich(ObjectNode value, String topic) {
            return value;
        }
    }
}
