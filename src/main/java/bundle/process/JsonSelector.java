package bundle.process;

import bundle.config.Configuration;
import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.typesafe.config.Config;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class JsonSelector implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final String SELECTOR_KEY = "selector";
    private static final String SELECTORS_KEY = "selectors";
    private final String selector;
    private JsonPointer pointer;

    protected JsonSelector(String selector) {
        this.selector = selector;
    }

    public JsonNode select(JsonNode object) {
        return object.at(getPointer());
    }

    public JsonNode selectParent(JsonNode object) {
        return object.at(getPointer().head());
    }

    public String fieldName() {
        return getPointer().last().toString();
    }

    public String getSelector() {
        return selector;
    }

    public JsonSelector getParentSelector() {
        return new JsonSelector(getPointer().head().toString());
    }

    protected JsonPointer getPointer() {
        // `com.fasterxml.jackson.core.JsonPointer` is not Serializable
        if (pointer == null) {
            pointer = JsonPointer.compile(getSelector());
        }
        return pointer;
    }

    /**
     * Parse a single selector from a string representation.
     */
    public static JsonSelector parse(String selector) {
        return new JsonSelector(selector);
    }

    /**
     * Parse a single selector from configuration.
     */
    public static JsonSelector parse(Config config) {
        if (!config.hasPath(SELECTOR_KEY)) {
            throw new RuntimeException("No selector element in given configuration");
        }
        final String selector = config.getString(SELECTOR_KEY);
        return parse(selector);
    }

    /**
     * Parse a single selector from configuration.
     */
    public static JsonSelector parse(Configuration configuration) {
        final Config config = configuration.getConfig();
        return parse(config);
    }

    /**
     * Parse many selectors from configuration.
     */
    public static List<JsonSelector> parseAll(Config config) {
        if (!config.hasPath(SELECTORS_KEY)) {
            // configuration does not have key for selectors list; assume single selector
            return Collections.singletonList(parse(config));
        }
        List<String> selectors = config.getStringList(SELECTORS_KEY);
        return selectors.stream().map(JsonSelector::new).collect(Collectors.toList());
    }

    /**
     * Parse many selectors from configuration.
     */
    public static List<JsonSelector> parseAll(Configuration configuration) {
        final Config config = configuration.getConfig();
        return parseAll(config);
    }

    @Override
    public String toString() {
        return "JsonSelector{" +
                "selector='" + selector + '\'' +
                '}';
    }
}
