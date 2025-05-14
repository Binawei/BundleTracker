package bundle.process.rules;

import bundle.config.RuleConfiguration;
import com.fasterxml.jackson.databind.JsonNode;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Rule testing a field value in a JSON object against regular expression patterns.
 */
public class JsonRegexRule extends JsonMatchingRule<Pattern> {
    private static final String PATTERNS_KEY = "patterns";
    private static final String PATTERN_KEY = "pattern";

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final List<Pattern> patterns;

    public JsonRegexRule(RuleConfiguration configuration) {
        super(configuration);

        final Config config = getConfiguration().getConfig();

        // compile list of regular expression patterns
        final List<String> patternStrings;
        if (config.hasPath(PATTERNS_KEY)) {
            patternStrings = config.getStringList(PATTERNS_KEY);
        } else {
            patternStrings = Collections.singletonList(config.getString(PATTERN_KEY));
        }
        patterns = patternStrings.stream().map(Pattern::compile).collect(Collectors.toList());
    }

    protected Predicate<Pattern> getPredicate(JsonNode node) {
        if (!node.isTextual()) {
            final String baseMessage = "Cannot perform regular expression match against a non-textual value";
            if (isStrict()) {
                logger.debug("{}; throwing error", baseMessage);
                throw new RuntimeException(baseMessage);
            }
            logger.debug("{}; ignoring", baseMessage);
            return pattern -> false;
        }

        final String value = node.textValue();
        return pattern -> pattern.matcher(value).find();
    }

    @Override
    protected Stream<Pattern> getPredicateStream(JsonNode node) {
        return patterns.stream();
    }
}
