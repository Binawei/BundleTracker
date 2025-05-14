package bundle.templates;

import org.apache.commons.text.lookup.StringLookup;

public interface TemplateLookup extends StringLookup {
    String lookup(String key);
    boolean isStrict();
}
