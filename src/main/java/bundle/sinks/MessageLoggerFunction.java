package bundle.sinks;

import com.typesafe.config.Config;
import org.apache.flink.shaded.netty4.io.netty.handler.logging.LogLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

import java.io.Serializable;

/**
 * Helper for any operators that require messages to be logged at a configurable log level.
 * @param <T> type of message to log
 */
public class MessageLoggerFunction<T> implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final String LOG_LEVEL_KEY = "level";

    private final Logger logger;
    private final LogLevel level;

    public MessageLoggerFunction(Class<?> clazz, LogLevel logLevel) {
        logger = LoggerFactory.getLogger(clazz);
        level = logLevel;
    }

    public MessageLoggerFunction(Class<?> clazz, Config config) {
        this(clazz, getLevel(config));
    }

    private static LogLevel getLevel(Config config) {
        if (config.hasPath(LOG_LEVEL_KEY)) {
            return LogLevel.valueOf(config.getString(LOG_LEVEL_KEY));
        }
        return LogLevel.INFO; // default
    }

    public void log(T value) {
        final String message = value.toString();
        switch (level) {
            case TRACE:
                logger.trace(message);
            case DEBUG:
                logger.debug(message);
                break;
            case INFO:
                logger.info(message);
                break;
            case WARN:
                logger.warn(message);
                break;
            case ERROR:
                logger.error(message);
                break;
        }
    }

    public void log(Marker marker, T value) {
        final String message = value.toString();
        switch (level) {
            case TRACE:
                logger.trace(marker, message);
            case DEBUG:
                logger.debug(marker, message);
                break;
            case INFO:
                logger.info(marker, message);
                break;
            case WARN:
                logger.warn(marker, message);
                break;
            case ERROR:
                logger.error(marker, message);
                break;
        }
    }

    public LogLevel getLevel() {
        return level;
    }
}
