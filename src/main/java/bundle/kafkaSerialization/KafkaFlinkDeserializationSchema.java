package bundle.kafkaSerialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

// TODO: implement using generics
public class KafkaFlinkDeserializationSchema implements KafkaDeserializationSchema<Tuple2<String, ObjectNode>>, Serializable {
    private static final long serialVersionUID = 1L;
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final Deserializer<String> keyDeserializer;
    private final Deserializer<ObjectNode> valueDeserializer;
    private final ObjectMapper mapper = new ObjectMapper();

    public KafkaFlinkDeserializationSchema(Deserializer<String> keyDeserializer, Deserializer<ObjectNode> valueDeserializer) {
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) {
        logger.debug("Opening Kafka deserialization schema");
    }

    @Override
    public boolean isEndOfStream(Tuple2<String, ObjectNode> nextElement) {
        return false;
    }

    @Override
    public Tuple2<String, ObjectNode> deserialize(ConsumerRecord<byte[], byte[]> record) {
        final String key = keyDeserializer.deserialize(record.topic(), record.key());
        final ObjectNode value = valueDeserializer.deserialize(record.topic(), record.value());
        return Tuple2.of(key, value);
    }

    @Override
    public TypeInformation<Tuple2<String, ObjectNode>> getProducedType() {
        return getProducedTypeHint().getTypeInfo();
    }

    public TypeHint<Tuple2<String, ObjectNode>> getProducedTypeHint() {
        return new TypeHint<Tuple2<String, ObjectNode>>(){};
    }
}
