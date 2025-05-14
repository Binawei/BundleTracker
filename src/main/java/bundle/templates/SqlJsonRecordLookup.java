package bundle.templates;

import bundle.helpers.IteratorHelper;
import bundle.helpers.JsonFieldFilterPredicate;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class SqlJsonRecordLookup extends JsonRecordLookup implements TemplateLookup {
    private static final String SET_LIST_KEY = "set-list";
    private static final String USING_TTL_KEY = "using-ttl";
    private static final String SEPARATOR = ", ";
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final SqlTemplateSetSelector sqlTemplateSetSelector;

    private final List<Map.Entry<String, JsonNode>> setListFields;

    private static final String ESCAPE_CHAR = "escape_char";
    private static final String ESCAPE_LIST = "escape_list";
    private HashMap<String, String> replacementValues;
    private String usingTTlSelector = null;
    private Long ttlDefault = null;
    private String setColumnQuoteChar = null;

    public SqlJsonRecordLookup(Tuple2<String, ObjectNode> record, SqlTemplateSetSelector sqlTemplateSetSelector, boolean strict, boolean autoQuote) {
        super(record, strict, autoQuote);
        this.sqlTemplateSetSelector = sqlTemplateSetSelector;
        setListFields = select(sqlTemplateSetSelector);
        replacementValues = new HashMap<>();
    }

    public SqlJsonRecordLookup(Tuple2<String, ObjectNode> record, SqlTemplateSetSelector sqlTemplateSetSelector, boolean strict, boolean autoQuote, @Nullable List<? extends Config> escapeList) {
        this(record, sqlTemplateSetSelector, strict, autoQuote);
        if (escapeList != null) {
            replacementValues = getMapFromConfig(escapeList);
        }
    }

    public SqlJsonRecordLookup(Tuple2<String, ObjectNode> record, SqlTemplateSetSelector sqlTemplateSetSelector, String ttlSelector, Long ttlDefault, boolean strict, boolean autoQuote, @Nullable List<? extends Config> escapeList, String setColumnQuoteChar) {
        this(record, sqlTemplateSetSelector, strict, autoQuote, escapeList);
        if (ttlSelector != null) {
            usingTTlSelector = ttlSelector;
        }
        if (ttlDefault != null) {
            this.ttlDefault = ttlDefault;
        }
        if (setColumnQuoteChar != null) {
            this.setColumnQuoteChar = setColumnQuoteChar;
        }
    }

    protected static HashMap<String, String> getMapFromConfig(List<? extends Config> configList) {
        HashMap<String, String> tempHashMap = new HashMap<>();
        for (Config anEscape : configList) {
            for (String charToEscape : anEscape.getStringList(ESCAPE_LIST)) {
                tempHashMap.put(charToEscape, anEscape.getString(ESCAPE_CHAR));
            }
        }
        return tempHashMap;
    }

    private String applyMapToValue(String value) {
        if (replacementValues.isEmpty()) {
            logger.debug("No escapes defined for text");
        } else {
            logger.debug(String.format("Replace text in string for value: %s", value));
            for (HashMap.Entry<String, String> entry : replacementValues.entrySet()) {
                String find = entry.getKey();
                String escape = entry.getValue();
                value = value.replace(find, escape + find);
            }
            logger.debug(String.format("New string value after replacements: %s", value));
        }
        return value;
    }

    @Override
    public String lookup(String key) {
        if (key.equalsIgnoreCase(SET_LIST_KEY)) {
            if (getSetListFields() == null) {
                String message = String.format("No set list value for selector '%s'", sqlTemplateSetSelector.getSelector());
                logger.error(message);
                throw new RuntimeException(message);
            }
            return buildSetListString(getSetListFields());
        }
        else if (key.equalsIgnoreCase(USING_TTL_KEY)) {
            Long ttlValue = null;

            // If no TTL selector, use default
            if (usingTTlSelector == null) {
                ttlValue = ttlDefault;
            }
            else
            {
                // Find value of TTL selector
                final JsonNode ttlNode = select(usingTTlSelector, getRecordValue());

                // If no value found, fallback on default. Provide warning.
                if (ttlNode.isMissingNode())
                {
                    if (ttlDefault == null) {
                        logger.warn(String.format("No ttl value for selector '%s'. No default value provided. No TTL will be specified.", usingTTlSelector));
                    }
                    else {
                        logger.warn(String.format("No ttl value for selector '%s'. Default TTL value of '%s' will be used.", usingTTlSelector, ttlDefault));
                        ttlValue = ttlDefault;
                    }
                }
                else
                {
                    ttlValue = Long.parseLong(ttlNode.asText()); // Parse allows TTL to be specified as string or numeric
                }
            }

            // Use TTL if we have one
            return ttlValue == null ? "" : String.format("USING TTL %s", ttlValue);
        }

        return super.lookup(key);
    }

    @Override
    protected String getQuoteDelimiter() {
        return "'";
    }

    @Override
    protected String getNullReplacementValue() {
        return "NULL";
    }

    protected SqlTemplateSetSelector getSqlTemplateSetSelector() {
        return sqlTemplateSetSelector;
    }

    @VisibleForTesting
    protected List<Map.Entry<String, JsonNode>> getSetListFields() {
        return setListFields;
    }

    protected List<Map.Entry<String, JsonNode>> select(SqlTemplateSetSelector sqlTemplateSetSelector) {
        if (sqlTemplateSetSelector == null) {
            return null;
        }
        JsonNode node = select(sqlTemplateSetSelector.getSelector(), getRecordValue());
        if (node.isMissingNode()) {
            return null; // strictness error check already handled
        }

        List<Map.Entry<String, JsonNode>> fields = IteratorHelper.toList(node.deepCopy().fields());
        Pattern filterPattern = this.sqlTemplateSetSelector.getPattern();
        if (filterPattern == null) {
            logger.debug("No filter pattern; returning {} fields", fields.size());
            return fields;
        }

        JsonFieldFilterPredicate filterPredicate = new JsonFieldFilterPredicate(filterPattern);

        List<Map.Entry<String, JsonNode>> filteredFields = fields.stream().filter(filterPredicate).collect(Collectors.toList());
        logger.debug("Returning {}/{} filtered fields", filteredFields.size(), fields.size());
        return filteredFields;
    }

    protected String buildSetListString(List<Map.Entry<String, JsonNode>> setListFields) {
        final StringBuilder builder = new StringBuilder();

        final int setListFieldCount = setListFields.size();
        for (int i = 0; i < setListFieldCount; i++) {
            Map.Entry<String, JsonNode> entry = setListFields.get(i);

            final String entryKey = entry.getKey();
            final JsonNode entryValue = entry.getValue();

            String replacementValue;
            if (entryValue.isNull() || entryValue.isMissingNode()) {
                replacementValue = getNullReplacementValue();
            }
            else if (entryValue.isTextual()) {
                // text values must be quoted
                replacementValue = quote(applyMapToValue(entryValue.asText()));
            } else {
                replacementValue = entryValue.asText();
            }
            if (setColumnQuoteChar == null)
                builder.append(String.format("%s=%s", entryKey, replacementValue));
            else
                builder.append(String.format("%s%s%s=%s", setColumnQuoteChar, entryKey, setColumnQuoteChar, replacementValue));

            if (i < setListFieldCount - 1) {
                // no separator for last field
                builder.append(SEPARATOR);
            }
        }
        return builder.toString();
    }
}
