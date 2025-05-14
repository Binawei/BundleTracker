package bundle.helpers;

import com.typesafe.config.Config;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ConfigurationHelper {
    private ConfigurationHelper() {}

    /**
     * Read content of a config section and its subsections as Java properties. This is usable for generic configuration
     * of Kafka consumer/producer or Hadoop client as part of Typesafe config configuration files with all its features.
     * <p>
     * <h3>Example</h3>
     * <pre><code>
     *    // Configuration properties of Kafka consumer as defined in https://kafka.apache.org/090/configuration.html
     *    kafka-consumer {
     *        bootstrap.servers = "localhost:9092"
     *        bootstrap.servers = ${?KAFKA_CONSUMER__BOOTSTRAP_SERVERS}
     *
     *        group.id = "my-consumer-group"
     *        group.id = ${?KAFKA_CONSUMER__GROUP_ID}
     *    }
     * </code></pre>
     *
     * @param config configuration section to be transformed to Properties
     * @return properties with the same content as in config object, both keys and values are strings
     *
     * Reference: https://github.com/lightbend/config/issues/357#issuecomment-306722506
     */
    public static Properties toProperties(Config config) {
        Properties properties = new Properties();
        config.entrySet().forEach(e -> properties.setProperty(e.getKey(), config.getString(e.getKey())));
        return properties;
    }

    public static Map<String, Object> toMap(Config config) {
        Map<String, Object> map = new HashMap<>();
        config.entrySet().forEach(e -> map.put(e.getKey(), config.getString(e.getKey())));
        return map;
    }
}
