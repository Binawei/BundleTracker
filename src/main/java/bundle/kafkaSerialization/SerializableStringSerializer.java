package bundle.kafkaSerialization;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Serializable;
import java.util.Map;

public class SerializableStringSerializer extends StringSerializer implements Serializer<String>, Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        super.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String topic, String data) {
        return super.serialize(topic, data);
    }
}
