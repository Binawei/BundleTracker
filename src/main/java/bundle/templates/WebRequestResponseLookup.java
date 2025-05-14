package bundle.templates;

import bundle.adapters.WebRequestResponse;
import org.apache.flink.annotation.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * String lookup operating on web request responses using JSON selectors.
 */
public class WebRequestResponseLookup extends BaseJsonStringLookup implements TemplateLookup {
    private static final String HTTP_STATUS_CODE_KEY = "code";
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final WebRequestResponse response;

    public WebRequestResponseLookup(WebRequestResponse response, boolean strict, boolean autoQuote) {
        super(strict, autoQuote);
        if (response == null) {
            final String baseMessage = "Response may not be null";
            logger.error("{}; throwing error", baseMessage);
            throw new RuntimeException(baseMessage);
        }
        this.response = response;
    }

    @Override
    public String lookup(String key) {
        if (key.equalsIgnoreCase(HTTP_STATUS_CODE_KEY)) {
            logger.trace("Returning response HTTP status code for selector '{}'", HTTP_STATUS_CODE_KEY);
            final int httpStatusCode = getResponse().getCode();
            return Integer.toString(httpStatusCode, 10);
        }
        return lookup(key, getResponse().getObject());
    }

    @VisibleForTesting
    public WebRequestResponse getResponse() {
        return response;
    }
}
