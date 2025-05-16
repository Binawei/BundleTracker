package bundle.source;

import bundle.config.SourceConfiguration;
import bundle.exceptions.ConfigurationException;
import bundle.exceptions.FactoryException;
import bundle.factory.SourceFactory;
import bundle.helpers.KafkaSourceTopicValidation;
import bundle.kafkaSerialization.KafkaFlinkDeserializationSchema;
import com.typesafe.config.Config;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

// TODO: fix generic type handling
@SuppressWarnings({"unused", "rawtypes"}) // dynamically constructed
public class KafkaSourceFactory<OUT> extends SourceFactory<OUT> {

    private static final String CONFIG_STARTUP_MODE_KEY = "startup.mode";
    private static final String CONFIG_STARTUP_TIMESTAMP_KEY = "startup.timestamp";
    private static final KafkaStartupMode DEFAULT_STARTUP_MODE = KafkaStartupMode.GROUP;
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final KafkaFlinkDeserializationSchema deserializationSchema;

    public KafkaSourceFactory(SourceConfiguration configuration) throws FactoryException {
        super(configuration);

        KafkaDeserializationHelper deserializationHelper;
        try {
            deserializationHelper = KafkaDeserializationHelper.of(configuration.getConfig());
        } catch (ConfigurationException exception) {
            final String message = "Error constructing Kafka serialization/deserialization helper";
            logger.error(message);
            throw new FactoryException(message, exception);
        }
        deserializationSchema = new KafkaFlinkDeserializationSchema(deserializationHelper.getKeyDeserializer(), deserializationHelper.getValueDeserializer());
    }

    @Override
    public SourceFunction<OUT> create() throws ConfigurationException {
        final SourceConfiguration configuration = getConfiguration();
        final Properties properties = configuration.getProperties();
        final Config config = configuration.getConfig();

        final KafkaStartupMode startupMode = getStartupMode(config);

        final KafkaSourceTopicValidation validator = new KafkaSourceTopicValidation();

        // create Kafka consumer
        FlinkKafkaConsumer<OUT> consumer;

        logger.info("Verifying Kafka topics configured");
        validator.verifyConfiguration(config);

        if (config.hasPath(validator.CONFIG_TOPIC_PATTERN_KEY)) {
            final String inputTopicPattern = config.getString(validator.CONFIG_TOPIC_PATTERN_KEY);
            final Pattern topicPattern = Pattern.compile(inputTopicPattern);
            logger.info("Creating Kafka source function consuming from topic pattern '{}'", inputTopicPattern);
            consumer = new FlinkKafkaConsumer<>(
                    topicPattern,
                    (KafkaDeserializationSchema<OUT>) deserializationSchema,
                    properties
            );

        } else {
            final List<String> inputTopicList;
            if (config.hasPath(validator.CONFIG_TOPIC_KEY)) {
                final String inputTopic = config.getString(validator.CONFIG_TOPIC_KEY);
                inputTopicList = Arrays.asList(inputTopic);
                logger.info("Creating Kafka source function consuming from topic '{}'", inputTopic);
            } else {
                inputTopicList = config.getStringList(validator.CONFIG_TOPICS_KEY);
                logger.info("Creating Kafka source function consuming from topic list '{}'", inputTopicList);
            }
            consumer = new FlinkKafkaConsumer<>(
                    inputTopicList,
                    (KafkaDeserializationSchema<OUT>) deserializationSchema,
                    properties
            );
        }

        switch(startupMode) {
            case GROUP:
                consumer.setStartFromGroupOffsets();
                break;
            case EARLIEST:
                consumer.setStartFromEarliest();
                break;
            case LATEST:
                consumer.setStartFromLatest();
                break;
            case TIMESTAMP:
                if (!config.hasPath(CONFIG_STARTUP_TIMESTAMP_KEY)) {
                    throw new ConfigurationException(String.format("Startup timestamp must be given in config key '%s' for timestamp startup mode",
                            CONFIG_STARTUP_TIMESTAMP_KEY));
                }
                final long startupTimestamp = config.getLong(CONFIG_STARTUP_TIMESTAMP_KEY);
                logger.info("Starting Kafka source function with startup timestamp {} (milliseconds)", startupTimestamp);
                consumer.setStartFromTimestamp(startupTimestamp);
                break;
            default:
                // sanity check; should not happen
                throw new RuntimeException(String.format("Unsupported started mode: %s", startupMode));
        }

        return consumer;
    }

    @Override
    public TypeHint<OUT> returns() {
        // TODO: handle generic output type correctly
        //noinspection unchecked
        return (TypeHint<OUT>) deserializationSchema.getProducedTypeHint();
    }

    private KafkaStartupMode getStartupMode(Config config) throws ConfigurationException {
        try {
            return config.hasPath(CONFIG_STARTUP_MODE_KEY) ?
                    KafkaStartupMode.parse(config.getString(CONFIG_STARTUP_MODE_KEY)) :
                    DEFAULT_STARTUP_MODE;
        } catch(IllegalArgumentException exception) {
            final String errorMessage = String.format("Error parsing startup mode from configuration value with key '%s'", CONFIG_STARTUP_MODE_KEY);
            logger.error(errorMessage, exception);
            throw new ConfigurationException(errorMessage, exception);
        }
    }
}
