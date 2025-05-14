package bundle.helpers;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Arrays;
import java.util.Optional;

/**
 * Helper functions around handling of temporal values.
 */
public class TemporalHelper {
    private static final Logger logger = LoggerFactory.getLogger(TemporalHelper.class);

    private static final DateTimeFormatter alternativeDateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private TemporalHelper() {}

    /**
     * Convert a date and time value to an ISO 8601 formatted string;
     */
    public static String toString(LocalDateTime localDateTime) {
        return localDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
    }

    /**
     * Parse an ISO 8601 formatted date string.
     * Handles {@link DateTimeFormatter#ISO_LOCAL_DATE_TIME}
     */
    public static LocalDateTime parseDateString(String dateString) {
        if (StringUtils.isNumeric(dateString)) {
            // if the date string is numeric then assume epoch date
            try {
                final long timestamp = Long.parseLong(dateString);
                return LocalDateTime.ofEpochSecond(timestamp, 0, ZoneOffset.UTC);
            } catch (NumberFormatException exception) {
                logger.error("Error parsing epoch date string '{}'", dateString);
                throw new DateTimeParseException("Error parsing epoch date string", dateString, 0, exception);
            }
        }

        try {
            return parseIsoDateString(dateString);
        } catch (DateTimeParseException exception) {
            logger.trace("Error parsing as ISO 8601 datetime string, attempting alternative format");
            return LocalDateTime.parse(dateString, alternativeDateTimeFormatter);
        }
    }

    public static TemporalUnit parseTemporalUnit(String code) {
        if (code == null) {
            throw new RuntimeException("Temporal unit code may not be null");
        }
        Optional<ChronoUnit> result = Arrays.stream(ChronoUnit.values())
                .filter(e -> e.name().equalsIgnoreCase(code))
                .findAny();
        if (!result.isPresent()) {
            throw new RuntimeException(String.format("Temporal unit code '%s'", code));
        }
        return result.get();
    }

    /**
     * Parse an ISO 8601 formatted date string.
     * Handles {@link DateTimeFormatter#ISO_LOCAL_DATE_TIME}
     */
    private static LocalDateTime parseIsoDateString(String dateString) throws DateTimeParseException {
        return LocalDateTime.parse(dateString);
    }
}
