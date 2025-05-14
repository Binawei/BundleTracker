package bundle.process;

import bundle.config.OperatorConfiguration;
import bundle.config.RuleConfiguration;
import bundle.exceptions.FactoryException;
import bundle.factory.RuleFactory;
import bundle.process.rules.JsonRule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;

public abstract class JsonProcessOperator extends ProcessOperator<Tuple2<String, ObjectNode>, Tuple2<String, ObjectNode>> implements Serializable
{
    private static final long serialVersionUID = 1L;
    private static final String RULE_KEY = "rule";
    private static final String RULES_KEY = "rules";
    private static final String RULES_OPERATOR_KEY = "rules_operator";

    private JsonRule rule = null;
    private ArrayList<JsonRule> rules;
    private String rules_operator;

    Config config = null;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    protected JsonProcessOperator(OperatorConfiguration.ProcessOperatorConfiguration configuration) {
        super(configuration);
        config = configuration.getConfig();
        if (config.hasPath(RULE_KEY))
        {
            RuleConfiguration ruleConfiguration = new RuleConfiguration(config.getConfig(RULE_KEY));
            rule = buildRule(ruleConfiguration);
        }
        else if(config.hasPath(RULES_KEY))
        {
            rules_operator = config.getString(RULES_OPERATOR_KEY);
            ArrayList<Config> ruleConfigurations = (ArrayList<Config>) config.getConfigList(RULES_KEY);
            rules = buildRules(ruleConfigurations);
        }
    }

    public Config getConfig(){
        return config;
    }

    protected JsonRule buildRule(RuleConfiguration ruleConfiguration) {
        try {
            return RuleFactory.getInstance().buildRule(ruleConfiguration);
        } catch (FactoryException exception) {
            logger.error("Error creating rule", exception);
            throw new RuntimeException("Error creating rule", exception);
        }
    }

    protected ArrayList<JsonRule> buildRules(ArrayList<Config> ruleConfigurations)
    {
         ArrayList<JsonRule> list = new ArrayList();
        try
        {
            for(int i = 0; i < ruleConfigurations.size(); i++)
            {
                list.add(RuleFactory.getInstance().buildRule(new RuleConfiguration(ruleConfigurations.get(i))));
            }
        }
        catch (FactoryException exception)
        {
            logger.error("Error creating rule", exception);
            throw new RuntimeException("Error creating rule", exception);
        }
        return list;
    }

    protected JsonRule getRule() {
        return rule;
    }
    protected ArrayList<JsonRule> getRules() {
        return rules;
    }


    public void processElement(Tuple2<String, ObjectNode> input, ProcessFunction<org.apache.flink.api.java.tuple.Tuple2<java.lang.String, com.fasterxml.jackson.databind.node.ObjectNode>, org.apache.flink.api.java.tuple.Tuple2<java.lang.String, com.fasterxml.jackson.databind.node.ObjectNode>>.Context context, Collector<Tuple2<String, ObjectNode>> collector) throws Exception
    {
        if (shouldSkipEvent(input)){
            collector.collect(input);
        }
        else{
            super.processElement(input, context, collector);
        }
    }

    protected boolean shouldSkipEvent(Tuple2<String, ObjectNode> input) throws Exception
    {
        logger.info("shouldSkipEvent triggered");
        System.out.println("shouldSkipEvent triggered");
        final String key = input.f0;
        final ObjectNode value = input.f1;

        if (!config.hasPath(RULE_KEY) && !config.hasPath(RULES_KEY))
        {
            return false;
        }

        if (config.hasPath(RULE_KEY))
        {
            return getRule().isSatisfiedBy(value);
        }
        else if(config.hasPath(RULES_KEY))
        {
            if(rules_operator.equals("AND"))
            {
                for(int i = 0; i < rules.size(); i++)
                {
                    if(!rules.get(i).isSatisfiedBy(value))
                    {
                        return false;
                    }
                }
                return true;
            }
            else
            {
                for(int i = 0; i < rules.size(); i++)
                {
                    if(rules.get(i).isSatisfiedBy(value))
                    {
                        return true;
                    }
                }
                return false;
            }
        }
        return false;
    }

    @Override
    public TypeHint<Tuple2<String, ObjectNode>> returns() {
        return new TypeHint<Tuple2<String, ObjectNode>>() {};
    }
}
