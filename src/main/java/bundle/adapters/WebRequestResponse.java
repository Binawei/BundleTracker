package bundle.adapters;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Wrapper for {@link WebRequestAdapter} response.
 */
public class WebRequestResponse {
    private final JsonNode object;
    private final int code;

    private WebRequestResponse(JsonNode object, int code) {
        this.object = object;
        this.code = code;
    }

    public static WebRequestResponse of(JsonNode object, int code) {
        return new WebRequestResponse(object, code);
    }

    public JsonNode getObject() {
        return object;
    }

    public int getCode() {
        return code;
    }
}
