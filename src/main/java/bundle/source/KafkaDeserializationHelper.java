package bundle.source;

import bundle.exceptions.ConfigurationException;
import bundle.kafkaSerialization.KafkaJsonObjectDeserializer;
import bundle.kafkaSerialization.SerializableStringDeserializer;
import bundle.source.enrichment.KafkaSourceEnrichment;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;
import org.apache.flink.util.function.SerializableSupplier;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.Serializable;

public class KafkaDeserializationHelper implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final Logger logger = LoggerFactory.getLogger(KafkaDeserializationHelper.class);

    // key
    private final Deserializer<String> keyDeserializer = new SerializableStringDeserializer();
    // value
    private transient Deserializer<ObjectNode> valueDeserializer = null;
    private final SerializableSupplier<Deserializer<ObjectNode>> valueDeserializerSupplier;

    private KafkaDeserializationHelper(SerializableSupplier<Deserializer<ObjectNode>> valueDeserializerSupplier) {
        this.valueDeserializerSupplier = valueDeserializerSupplier;
    }

    public static KafkaDeserializationHelper of(Config config) throws ConfigurationException {
        final SerializableSupplier<Deserializer<ObjectNode>> valueDeserializerSupplier;

        // enrichment
        Config enrichmentConfig = null;
        if (config.hasPath("enrichment")) {
            enrichmentConfig = config.getConfig("enrichment");
        }
        final KafkaSourceEnrichment sourceEnrichment = KafkaSourceEnrichment.of(enrichmentConfig);


            valueDeserializerSupplier = new SerializableSupplier<Deserializer<ObjectNode>>() {
                private static final long serialVersionUID = 1L;

                @Override
                public Deserializer<ObjectNode> get() {
                    logger.info("No schema registry; using JSON for value deserialization");
                    return new KafkaJsonObjectDeserializer(sourceEnrichment);
                }
            };

        return new KafkaDeserializationHelper(valueDeserializerSupplier);
    }

    public Deserializer<String> getKeyDeserializer() {
        return keyDeserializer;
    }

    public synchronized Deserializer<ObjectNode> getValueDeserializer() {
        // to handle the deserializer not being serializable
        if (valueDeserializer == null) {
            valueDeserializer = valueDeserializerSupplier.get();
        }
        return valueDeserializer;
    }
}
