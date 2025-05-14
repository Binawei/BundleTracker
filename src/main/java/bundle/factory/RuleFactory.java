package bundle.factory;

import bundle.config.RuleConfiguration;
import bundle.exceptions.FactoryException;
import bundle.process.rules.JsonRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RuleFactory extends ComponentFactory {
    private static RuleFactory INSTANCE;
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private static final String RULE_CLASS_KEY = "class";

    private RuleFactory() { }

    public synchronized static RuleFactory getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new RuleFactory();
        }
        return INSTANCE;
    }

    public JsonRule buildRule(RuleConfiguration ruleConfiguration) throws FactoryException {
        String ruleClassName = ruleConfiguration.getConfig().getString(RULE_CLASS_KEY);

        Object rule = construct(ruleClassName, ruleConfiguration);

        if (!(rule instanceof JsonRule)) {
            final String message = String.format("Invalid rule implementation with type '%s'", rule.getClass().getName());
            logger.error(message);
            throw new FactoryException(message);
        }

        return (JsonRule) rule;
    }
}
