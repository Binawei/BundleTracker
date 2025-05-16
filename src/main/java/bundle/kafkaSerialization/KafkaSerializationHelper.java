package bundle.kafkaSerialization;

import bundle.exceptions.ConfigurationException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;
import org.apache.flink.util.function.SerializableSupplier;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class KafkaSerializationHelper implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final Logger logger = LoggerFactory.getLogger(KafkaSerializationHelper.class);

    // key
    private final Serializer<String> keySerializer = new SerializableStringSerializer();
    // value
    private transient Serializer<ObjectNode> valueSerializer = null;
    private final SerializableSupplier<Serializer<ObjectNode>> valueSerializerSupplier;

    private KafkaSerializationHelper(SerializableSupplier<Serializer<ObjectNode>> valueSerializerSupplier) {
        this.valueSerializerSupplier = valueSerializerSupplier;
    }

    public static KafkaSerializationHelper of(Config config) throws ConfigurationException {
        final SerializableSupplier<Serializer<ObjectNode>> valueSerializerSupplier;

            valueSerializerSupplier = new SerializableSupplier<Serializer<ObjectNode>>()  {
                private static final long serialVersionUID = 1L;

                @Override
                public Serializer<ObjectNode> get() {
                    logger.info("No schema registry; using JSON for value deserialization");
                    return new KafkaJsonObjectSerializer();
                }
            };

        return new KafkaSerializationHelper(valueSerializerSupplier);
    }
    public Serializer<String> getKeySerializer() {
        return keySerializer;
    }

    public synchronized Serializer<ObjectNode> getValueSerializer() {
        // to handle the serializer not being serializable
        if (valueSerializer == null) {
            valueSerializer = valueSerializerSupplier.get();
        }
        return valueSerializer;
    }

}
