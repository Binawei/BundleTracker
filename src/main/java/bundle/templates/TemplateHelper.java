package bundle.templates;

public class TemplateHelper {
    private TemplateHelper() {}

    public static String cleanKey(String key) {
        if (key == null) {
            return null;
        }
        final String cleanedKey = key.trim();
        if (cleanedKey.equals("")) {
            return null;
        }
        return cleanedKey;
    }
}
