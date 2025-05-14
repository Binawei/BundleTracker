package bundle.process.rules;

import bundle.config.RuleConfiguration;
import com.fasterxml.jackson.databind.node.ObjectNode;


/**
 * Base class for any rules operating on JSON objects.
 */
public abstract class JsonRule extends BaseRule<ObjectNode> implements Rule<ObjectNode> {
    protected JsonRule(RuleConfiguration configuration) {
        super(configuration);
    }
}
