package bundle.templates;

import bundle.adapters.WebRequestResponse;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;
import org.apache.commons.text.StringSubstitutor;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StringResolverFactory implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final String RECORD_LOOKUP_KEY = "record";
    private static final String RESPONSE_LOOKUP_KEY = "response";

    private static final String PREFIX_KEY = "prefix";
    private static final String SUFFIX_KEY = "suffix";
    private static final String ESCAPE_KEY = "escape";
    private static final String STRICT_KEY = "strict";
    private static final String AUTO_QUOTE_KEY = "auto.quote";

    private String prefix = StringSubstitutor.DEFAULT_VAR_START;
    private String suffix = StringSubstitutor.DEFAULT_VAR_END;
    private char escape = StringSubstitutor.DEFAULT_ESCAPE;
    private boolean strict = false;
    private boolean autoQuote = true;

    private StringResolverFactory(@Nullable Config resolverConfig) {
        if (resolverConfig != null) {
            if (resolverConfig.hasPath(PREFIX_KEY)) {
                prefix = resolverConfig.getString(PREFIX_KEY);
            }
            if (resolverConfig.hasPath(SUFFIX_KEY)) {
                suffix = resolverConfig.getString(SUFFIX_KEY);
            }
            if (resolverConfig.hasPath(ESCAPE_KEY)) {
                escape = resolverConfig.getString(ESCAPE_KEY).charAt(0);
            }
            strict = resolverConfig.hasPath(STRICT_KEY) && resolverConfig.getBoolean(STRICT_KEY);
            autoQuote = !resolverConfig.hasPath(AUTO_QUOTE_KEY) || resolverConfig.getBoolean(AUTO_QUOTE_KEY);
        }
    }

    public static StringResolverFactory get(@Nullable Config resolverConfig) {
        return new StringResolverFactory(resolverConfig);
    }

    /**
     * Create a string resolver providing template lookups from a single JSON record as source.
     */
    public StringSubstitutor of(Tuple2<String, ObjectNode> record) {
        return build(new JsonRecordLookup(record, isStrict(), shouldAutoQuote()));
    }

    /**
     * Create a string resolver providing template lookups from either a JSON record or response.
     */
    public StringSubstitutor of(Tuple2<String, ObjectNode> record, WebRequestResponse response) {
        final Map<String, TemplateLookup> lookupMap = new HashMap<String, TemplateLookup>(){{
            put(RECORD_LOOKUP_KEY, new JsonRecordLookup(record, isStrict(), shouldAutoQuote()));
            put(RESPONSE_LOOKUP_KEY, new WebRequestResponseLookup(response, isStrict(), shouldAutoQuote()));
        }};
        return build(lookupMap);
    }

    public StringSubstitutor of(Tuple2<String, ObjectNode> record, SqlTemplateSetSelector sqlTemplateSetSelector) {
        return build(new SqlJsonRecordLookup(record, sqlTemplateSetSelector, isStrict(), shouldAutoQuote()));
    }

    public StringSubstitutor of(Tuple2<String, ObjectNode> record, SqlTemplateSetSelector sqlTemplateSetSelector, @Nullable List<? extends Config> replaceValues) {
        return build(new SqlJsonRecordLookup(record, sqlTemplateSetSelector, isStrict(), shouldAutoQuote(), replaceValues));
    }

    public StringSubstitutor of(Tuple2<String, ObjectNode> record, SqlTemplateSetSelector sqlTemplateSetSelector, String ttlSelector, Long ttlDefault, @Nullable List<? extends Config> replaceValues, String setColumnQuoteChar) {
        return build(new SqlJsonRecordLookup(record, sqlTemplateSetSelector, ttlSelector, ttlDefault, isStrict(), shouldAutoQuote(), replaceValues, setColumnQuoteChar));
    }

    private StringSubstitutor build(Map<String, TemplateLookup> lookupMap) {
        final TemplateLookup templateLookup = new MapStringLookup(lookupMap, isStrict());
        return build(templateLookup);
    }

    private StringSubstitutor build(TemplateLookup lookup) {
        return new StringSubstitutor(lookup, prefix, suffix, escape);
    }

    public boolean isStrict() {
        return strict;
    }

    @VisibleForTesting
    public boolean shouldAutoQuote() {
        return autoQuote;
    }
}
