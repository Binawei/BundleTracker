package bundle.templates;

import bundle.process.JsonSelector;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Base String lookup operating on JSON messages using JSON selectors.
 */
public abstract class BaseJsonStringLookup implements TemplateLookup {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final boolean strict;
    private final boolean autoQuote;
    private final String quoteDelimiter;

    protected BaseJsonStringLookup(boolean strict, boolean autoQuote) {
        this.strict = strict;
        this.autoQuote = autoQuote;
        this.quoteDelimiter = getQuoteDelimiter();
    }

    protected JsonNode select(String rawKey, JsonNode node) {
        final String key = TemplateHelper.cleanKey(rawKey);

        if (key == null) {
            // configuration issue; therefore fatal
            final String baseMessage = "Lookup key is not set";
            logger.error("{}; throwing error", baseMessage);
            throw new RuntimeException(baseMessage);
        }

        JsonSelector selector = JsonSelector.parse(key);
        JsonNode selectedNode = selector.select(node);
        if (isStrict() && selectedNode.isMissingNode()) {
            final String baseMessage = String.format("No node found using selector '%s' in strict mode", selector.getSelector());
            logger.error("{}; throwing error", baseMessage);
            throw new RuntimeException(baseMessage);
        }

        return selectedNode;
    }

    protected String lookup(String rawKey, JsonNode node) {
        JsonNode selectedNode = select(rawKey, node);

        if (selectedNode.isTextual() && shouldAutoQuote()) {
            return quote(selectedNode);
        }

        return (selectedNode.isNull() || selectedNode.isMissingNode()) ? getNullReplacementValue() : selectedNode.asText();
    }

    @Override
    public boolean isStrict() {
        return strict;
    }

    protected boolean shouldAutoQuote() {
        return autoQuote;
    }

    /**
     * Get value to represent `null` in the output template.
     */
    protected String getNullReplacementValue() {
        return "null";
    }

    protected String quote(JsonNode node) {
        return quote(node.asText());
    }

    protected String quote(String text) {
        return text == null ? getNullReplacementValue() : String.format("%s%s%s", quoteDelimiter, text, quoteDelimiter);
    }

    protected String getQuoteDelimiter() {
        return "\"";
    }
}
