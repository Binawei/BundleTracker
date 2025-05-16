package bundle.kafkaSerialization;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Serializable;
import java.util.Map;

public class SerializableStringDeserializer extends StringDeserializer implements Deserializer<String>, Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        super.configure(configs, isKey);
    }

    @Override
    public String deserialize(String topic, byte[] data) {
        return super.deserialize(topic, data);
    }
}
