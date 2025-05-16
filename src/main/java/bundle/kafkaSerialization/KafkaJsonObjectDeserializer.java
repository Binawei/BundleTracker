package bundle.kafkaSerialization;

import bundle.source.enrichment.KafkaSourceEnrichment;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.Serializable;

public class KafkaJsonObjectDeserializer implements Deserializer<ObjectNode>, Serializable {
    private static final long serialVersionUID = 1L;
    private static final ObjectMapper mapper = new ObjectMapper();

    private final KafkaSourceEnrichment enrichment;

    public KafkaJsonObjectDeserializer(KafkaSourceEnrichment enrichment){
        super();
        this.enrichment = enrichment;
    }

    @Override
    public ObjectNode deserialize(String topic, byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        try {
            final JsonNode node = mapper.readTree(bytes);
            return enrichment.enrich(node.deepCopy(), topic);
        } catch (Exception exception) {
            throw new SerializationException(exception);
        }
    }
}
