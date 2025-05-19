package bundle.kafkaSerialization;

import bundle.process.JsonSelector;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Optional;

public class KafkaFlinkSerializationSchema implements KafkaSerializationSchema<Tuple2<String, ObjectNode>> {
    private static final long serialVersionUID =1L;
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private static final String TOPIC_SELECTOR = "/output_topic";
    private final Serializer<String> keySerializer;
    private final Serializer<ObjectNode> valueSerializer;
    private final String defaultTopic;

    public KafkaFlinkSerializationSchema(Serializer<String> keySerializer, Serializer<ObjectNode> valueSerializer, String defaultTopic) {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.defaultTopic = defaultTopic;
    }


    @Override
    public void open(SerializationSchema.InitializationContext context) throws Exception {
        logger.info(" Openning kafka for serialization");
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Tuple2<String, ObjectNode> record, @Nullable Long aLong) {
        try {
            final String key = record.f0;
            final ObjectNode value = record.f1;
            String topic;
            final JsonSelector selector = JsonSelector.parse(TOPIC_SELECTOR);
            topic = selector.select(value).asText();
            if (Optional.ofNullable(selector.select(value).asText()).filter(s -> s.isEmpty()).isPresent()) {
                topic = defaultTopic;
            }
            logger.info("Output topic is {}", topic);
            final byte[] keyBytes = keySerializer.serialize(topic, key);
            final byte[] valueBytes = valueSerializer.serialize(topic, value);
            return new ProducerRecord<>(topic, keyBytes, valueBytes);
        } catch (Exception e){
            logger.error("Unable to serialize record: %s", record, e);
            return null;
        }
    }
}
