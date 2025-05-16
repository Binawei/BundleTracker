package bundle.source;

import java.util.Arrays;
import java.util.Optional;

/**
 * Offset startup mode for Kafka consumers.
 * Based on Flink internal {@link org.apache.flink.streaming.connectors.kafka.config.StartupMode}.
 */
public enum KafkaStartupMode {

    /**
     * Start from committed offset for consumer group in Kafka/ZooKeeper.
     */
    GROUP("GROUP"),

    /**
     * Start from the earliest possible offset.
     */
    EARLIEST("EARLIEST"),

    /**
     * Start from the latest offset.
     */
    LATEST("LATEST"),

    /**
     * Start from a specific offset timestamp.
     * Startup offset timestamp (in milliseconds) is required when using this mode.
     */
    TIMESTAMP("TIMESTAMP");

    private final String code;

    KafkaStartupMode(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    @Override
    public String toString() {
        return "KafkaStartupMode{" +
                "code='" + code + '\'' +
                '}';
    }

    public static KafkaStartupMode parse(String startupModeCode) {
        if (startupModeCode == null) {
            throw new IllegalArgumentException("Startup mode code may not be null");
        }
        Optional<KafkaStartupMode> result = Arrays.stream(KafkaStartupMode.values())
                .filter(e -> e.name().equalsIgnoreCase(startupModeCode))
                .findAny();
        if (!result.isPresent()) {
            throw new IllegalArgumentException(String.format("Unsupported startup mode code '%s'", startupModeCode));
        }
        return result.get();
    }
}
