package bundle.helpers;

import bundle.process.JsonSelector;
import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.function.Consumer;

/**
 * Convenience class that allows easy mutation of Object Node.
 */
public class ObjectNodeWrapper implements Serializable {
    private static final long serialVersionUID = 1L;
    private final ObjectNode objectNode;
    private final ObjectMapper objectMapper;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    public ObjectNodeWrapper(ObjectNode objectNode) {
        this.objectNode = objectNode;
        objectMapper = new ObjectMapper();
    }

    private String getFieldName(JsonSelector selector) {
        String fieldName = selector.fieldName();
        return fieldName.replace(Character.toString(JsonPointer.SEPARATOR), StringUtils.EMPTY);
    }

    private void performAction(JsonSelector selector, Consumer<ObjectNode> action) {
        ObjectNode objectNode = (ObjectNode)selector.selectParent(getObjectNode());

        if (!objectNode.isMissingNode() && objectNode.isObject()) {
            action.accept(objectNode);
        }
    }

    public JsonNode select(JsonSelector selector) {
        return selector.select(getObjectNode());
    }

    public JsonNode select(String selector) {
        return select(JsonSelector.parse(selector));
    }

    public void remove(String selector) {
        remove(JsonSelector.parse(selector));
    }

    public void remove(JsonSelector selector) {
        performAction(selector, objectNode -> objectNode.remove(getFieldName(selector)));
    }

    public void put(String selector, long value) {
        put(JsonSelector.parse(selector), value);
    }

    public void put(String selector, int value) {
        put(JsonSelector.parse(selector), value);
    }

    public void put(String selector, String value) {
        put(JsonSelector.parse(selector), value);
    }

    public void put(String selector, JsonNode value) {
        put(JsonSelector.parse(selector), value);
    }

    private void resolvePath(JsonSelector selector) {

        if (selector.select(objectNode).isMissingNode()) {
            logger.trace("Path does not exists for {}", selector.getSelector());
            resolvePath(selector.getParentSelector());
            logger.trace("Placing path: {}", selector.getSelector());
            // TODO: fix `JsonNode.put` method deprecation
            performAction(selector, objectNode -> objectNode.put(getFieldName(selector), objectMapper.createObjectNode()));
            logger.trace("New object: {}", objectNode);
        } else {
            logger.trace("Path exists for {}", selector.getSelector());
        }
    }

    public void put(JsonSelector selector, long value) {
        resolvePath(selector.getParentSelector());
        performAction(selector, objectNode -> objectNode.put(getFieldName(selector), value));
    }

    public void put(JsonSelector selector, int value) {
        resolvePath(selector.getParentSelector());
        performAction(selector, objectNode -> objectNode.put(getFieldName(selector), value));
    }

    public void put(JsonSelector selector, String value) {
        resolvePath(selector.getParentSelector());
        performAction(selector, objectNode -> objectNode.put(getFieldName(selector), value));
    }

    public void put(JsonSelector selector, JsonNode value) {
        resolvePath(selector.getParentSelector());
        performAction(selector, objectNode -> objectNode.set(getFieldName(selector), value));
    }

    public ObjectNode getObjectNode() {
        return objectNode;
    }
}
