package bundle.helpers;

import bundle.exceptions.ConfigurationException;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Stream;

public class KafkaSourceTopicValidation {

    public final String CONFIG_TOPIC_KEY = "topic";
    public final String CONFIG_TOPICS_KEY = "topics";
    public final String CONFIG_TOPIC_PATTERN_KEY = "topic_pattern";

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final String errorMessage = String.format("Input topic required in either ONE of the configuration keys '%s', '%s' or '%s'", CONFIG_TOPIC_KEY, CONFIG_TOPICS_KEY, CONFIG_TOPIC_PATTERN_KEY);
    private final String noConfigErrorMessage = "No topic configuration found, this should not be possible as it has already been verified in KafkaSourceTopicValidation";

    public void verifyConfiguration(Config config) throws ConfigurationException {

        final Boolean hasTopic = config.hasPath(CONFIG_TOPIC_KEY);
        final Boolean hasTopics = config.hasPath(CONFIG_TOPICS_KEY);
        final Boolean hasTopicPattern = config.hasPath(CONFIG_TOPIC_PATTERN_KEY);

        long count = Stream.of(hasTopic, hasTopics, hasTopicPattern).filter(p -> p).count();

        if (count != 1) {
            logger.error(errorMessage);
            throw new ConfigurationException(errorMessage);
        }
    }

    // This Exception is for after this class has validated config and values read from outside defies it. Should be unreachable call
    public IllegalStateException illegalStateConfigError() {
        return new IllegalStateException(noConfigErrorMessage);
    }

}
