package bundle.helpers;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * Predicate to filter JSON fields (key and value).
 */
public class JsonFieldFilterPredicate implements Predicate<Map.Entry<String, JsonNode>> {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Pattern fieldNamePattern;

    public JsonFieldFilterPredicate(Pattern fieldNamePattern) {
        this.fieldNamePattern = fieldNamePattern;
    }

    public JsonFieldFilterPredicate(String fieldNamePattern) {
        this(Pattern.compile(fieldNamePattern));
    }

    protected Pattern getFieldNamePattern() {
        return fieldNamePattern;
    }

    @Override
    public boolean test(Map.Entry<String, JsonNode> field) {
        final String fieldName = field.getKey();

        if (fieldName == null) {
            // safety check, should not happen
            logger.warn("Ignoring field with null name");
            return false;
        }

        boolean matches = getFieldNamePattern().matcher(fieldName).find();
        logger.trace("Field with name '{}' {} pattern {}",
                fieldName, matches ? "matches" : "does not match", getFieldNamePattern());
        return matches;
    }
}
