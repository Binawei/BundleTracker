package bundle.process.rules;

import bundle.config.RuleConfiguration;
import bundle.process.JsonSelector;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;
/**
 * Base class for any rules operating on JSON objects with a field selector (JSON Pointer).
 */
public abstract class JsonSelectorRule extends JsonRule {
    private static final String STRICT_KEY = "strict";
    private static final String INVERSE_KEY = "inverse";
    protected final JsonSelector selector;
    private final boolean strict;
    private final boolean inverse;

    protected JsonSelectorRule(RuleConfiguration configuration) {
        super(configuration);

        final Config config = getConfiguration().getConfig();
        strict = config.hasPath(STRICT_KEY) && config.getBoolean(STRICT_KEY);
        inverse = config.hasPath(INVERSE_KEY) && config.getBoolean(INVERSE_KEY);
        selector = JsonSelector.parse(configuration);
    }

    protected abstract boolean isSatisfiedBySelectedNode(JsonNode node);

    @Override
    public boolean isSatisfiedBy(ObjectNode object) {
        final JsonNode node = selector.select(object);
        // XOR operator used to invert the boolean of isSatisfiedBySelectedNode if inverse is true
        return (inverse ^ isSatisfiedBySelectedNode(node));
    }

    protected boolean isStrict() {
        return strict;
    }
}
