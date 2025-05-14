package bundle.adapters;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

public interface WebRequestAdapter extends Serializable {
    WebRequestResponse get(String url, Map<String, String> headers) throws IOException;
    WebRequestResponse post(String url, Map<String, String> headers, JsonNode payload) throws IOException;
    WebRequestResponse put(String url, Map<String, String> headers, JsonNode payload) throws IOException;
    WebRequestResponse delete(String url, Map<String, String> headers) throws IOException;

    enum Method {
        GET("GET"),
        POST("POST"),
        PUT("PUT"),
        DELETE("DELETE");

        private final String code;

        Method(String code) {
            this.code = code;
        }

        public String getCode() {
            return code;
        }
    }

    enum ResponseTransform {
        INPUT("INPUT"),
        INPUT_RESPONSE("INPUT_RESPONSE"),
        RESPONSE("RESPONSE"),
        RESPONSE_INPUT("RESPONSE_INPUT"),
        CUSTOM("CUSTOM");

        private final String option;

        ResponseTransform(String option) {
            this.option = option;
        }

        public String getOption() {
            return option;
        }

        public static ResponseTransform parse(String transformOption) {
            if (transformOption == null) {
                return CUSTOM;
            }
            Optional<ResponseTransform> result = Arrays.stream(ResponseTransform.values())
                    .filter(e -> e.name().equalsIgnoreCase(transformOption))
                    .findAny();
            if (!result.isPresent()) {
                throw new RuntimeException(String.format("Unsupported response transform '%s'", transformOption));
            }
            return result.get();
        }
    }
}
