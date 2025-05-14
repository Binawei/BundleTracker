package bundle.templates;

import org.apache.flink.annotation.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class MapStringLookup implements TemplateLookup {
    private final static String LOOKUP_KEY_SEPARATOR = ":";
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Map<String, TemplateLookup> lookupMap;
    private final boolean strict;

    public MapStringLookup(Map<String, TemplateLookup> lookupMap, boolean strict) {
        if (lookupMap == null) {
            final String baseMessage = "Lookup map must not be null";
            logger.error("{}; throwing error", baseMessage);
            throw new RuntimeException(baseMessage);
        }
        if (lookupMap.size() == 0) {
            final String baseMessage = "Lookup map must contain at least one element";
            logger.error("{}; throwing error", baseMessage);
            throw new RuntimeException(baseMessage);
        }
        this.lookupMap = lookupMap;
        this.strict = strict;
    }

    @Override
    public String lookup(String rawKey) {
        final String key = TemplateHelper.cleanKey(rawKey);

        if (key == null) {
            // configuration issue; therefore fatal
            final String baseMessage = "Lookup key is not set";
            logger.error("{}; throwing error", baseMessage);
            throw new RuntimeException(baseMessage);
        }

        logger.debug("Performing map lookup using key: {}", key);

        //final Tuple2<String, String> keyComponents = parseKey(key);
        final String[] keyComponents = key.split(LOOKUP_KEY_SEPARATOR);
        if (keyComponents.length != 2) {
            // configuration issue; therefore fatal
            final String baseMessage = String.format("Invalid look up key '%s'; must contain two components", key);
            logger.error("{}; throwing error", baseMessage);
            throw new RuntimeException(baseMessage);
        }
        final TemplateLookup templateLookup = getTemplateLookup(TemplateHelper.cleanKey(keyComponents[0]));
        if (templateLookup == null) {
            // configuration issue; therefore fatal
            final String baseMessage = "Cannot find lookup in map";
            logger.error("{}; throwing error", baseMessage);
            throw new RuntimeException(baseMessage);
        }
        return templateLookup.lookup(TemplateHelper.cleanKey(keyComponents[1]));
    }

    @Override
    public boolean isStrict() {
        return strict;
    }

    private TemplateLookup getTemplateLookup(String mapKey) {
        if (mapKey == null) {
            final String baseMessage = "No lookup map key provided";
            logger.error("{}; throwing error", baseMessage);
            throw new RuntimeException(baseMessage);
        }
        return getLookupMap().get(mapKey);
    }

    @VisibleForTesting
    public Map<String, TemplateLookup> getLookupMap() {
        return lookupMap;
    }
}
