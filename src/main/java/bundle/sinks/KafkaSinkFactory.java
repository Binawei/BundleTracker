package bundle.sinks;

import bundle.config.SinkConfiguration;
import bundle.exceptions.ConfigurationException;
import bundle.factory.SinkFactory;
import bundle.kafkaSerialization.KafkaFlinkSerializationSchema;
import bundle.kafkaSerialization.KafkaSerializationHelper;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;


public class KafkaSinkFactory<IN> extends SinkFactory<IN> {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final KafkaSerializationHelper serializationHelper;
    private static final String  TRANSACTIONAL_ID_PREFIX_KEY = "transactional.id.prefix";

    public KafkaSinkFactory(SinkConfiguration configuration) throws ConfigurationException {
        super(configuration);
        serializationHelper = KafkaSerializationHelper.of(configuration.getConfig());
    }

    @Override
    public SinkFunction<IN> create() {
        final SinkConfiguration configuration = getConfiguration();
        final Properties properties = configuration.getProperties();
        final String SinkTopic = configuration.getConfig().getString("topic");
        logger.info("Creating a sink function producing to output topic : {}", SinkTopic);
        KafkaSerializationSchema<IN> serializationSchema = getSerializationSchema(SinkTopic);
        logger.info("Npw creating kafka producer in flink connector");
        FlinkKafkaProducer<IN> flinkProducer = new FlinkKafkaProducer<>(SinkTopic, serializationSchema, properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        setTransactionalIdPrfix(getConfiguration().getProperties(), flinkProducer);
        return flinkProducer;
    }

    private void setTransactionalIdPrfix(Properties properties, FlinkKafkaProducer<IN> flinkProducer) {
        if (properties != null && properties.contains(TRANSACTIONAL_ID_PREFIX_KEY)){
            String transactioanlIdPrefix = String.format("%s-%s", properties.getProperty(TRANSACTIONAL_ID_PREFIX_KEY),
                    UUID.randomUUID());
            flinkProducer.setTransactionalIdPrefix(transactioanlIdPrefix);
        }
    }

    private KafkaSerializationSchema<IN> getSerializationSchema(String sinkTopic) {
        KafkaFlinkSerializationSchema serializationSchema = new KafkaFlinkSerializationSchema(serializationHelper.getKeySerializer(),
                serializationHelper.getValueSerializer(), sinkTopic);
        return (KafkaSerializationSchema<IN>) serializationSchema;
    }


}
