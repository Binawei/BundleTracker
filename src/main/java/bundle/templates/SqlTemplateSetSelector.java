package bundle.templates;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.regex.Pattern;

/**
 * Typed marker class for {@link SqlJsonRecordLookup} wrapping a string selector value.
 */
public class SqlTemplateSetSelector implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String selector;
    private final Pattern pattern;

    private SqlTemplateSetSelector(String selector, @Nullable Pattern pattern) {
        this.selector = selector;
        this.pattern = pattern;
    }

    public String getSelector() {
        return selector;
    }

    public Pattern getPattern() {
        return pattern;
    }

    public static SqlTemplateSetSelector of(@Nullable String selector, @Nullable Pattern pattern) {
        if (selector == null || selector.trim().equals("")) {
            return null;
        }
        return new SqlTemplateSetSelector(selector, pattern);
    }

    public static SqlTemplateSetSelector of(@Nullable String selector) {
        return of(selector, null);
    }

    @Override
    public String toString() {
        return getSelector();
    }
}
