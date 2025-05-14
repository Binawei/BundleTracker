package bundle.process;

import bundle.config.OperatorConfiguration;
import bundle.config.RuleConfiguration;
import bundle.exceptions.ConfigurationException;
import bundle.process.rules.JsonRule;
import bundle.templates.StringResolverFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.apache.commons.text.StringSubstitutor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;


public class JsonObjectConditionalTransformProcessOperator extends JsonProcessOperator
{
    private static final long serialVersionUID = 1L;
    private static final String RESOLVER_KEY = "resolver";
    private static final String AUTO_QUOTE_KEY = "auto.quote";

    private static final String TRANSFORM_RULES_KEY = "transformRules";
    private static final String TRANSFORM_KEY_KEY = "key";
    private static final String TRANSFORM_VALUE_KEY = "value";

    private static final String ADDITIONAL_KEY = "additional";
    private static final String RULE_KEY = "rule";
    private static final String RULES_KEY = "rules";
    private static final String RULES_OPERATOR_KEY = "rules_operator";

    private static StringResolverFactory stringResolverFactory = null;
    private static StringResolverFactory keyStringResolverFactory = null;
    private static final ObjectMapper mapper = new ObjectMapper();
    private String rules_operator;

    private ArrayList<Config> outputConfigTransformRules;
    private ArrayList<String> responseTemplateTransform;

    private static final Logger logger = LoggerFactory.getLogger(JsonObjectConditionalTransformProcessOperator.class);

    private JsonRule rule = null;

    public JsonObjectConditionalTransformProcessOperator(OperatorConfiguration.ProcessOperatorConfiguration configuration) throws ConfigurationException
    {
        super(configuration);
    }

    @Override
    protected void processElementInternal(Tuple2<String, ObjectNode> input, Context context, Collector<Tuple2<String, ObjectNode>> collector) throws Exception
    {
        final Config resolverConfig = super.getConfig().hasPath(RESOLVER_KEY) ? super.getConfig().getConfig(RESOLVER_KEY) : ConfigFactory.empty();
        final Config keyResolverConfig = resolverConfig != null ? resolverConfig.withValue(AUTO_QUOTE_KEY, ConfigValueFactory.fromAnyRef(false)) : null;
        stringResolverFactory = StringResolverFactory.get(resolverConfig);
        keyStringResolverFactory = StringResolverFactory.get(keyResolverConfig);

        outputConfigTransformRules = super.getConfig().hasPath(TRANSFORM_RULES_KEY) ? (ArrayList)this.config.getConfigList(TRANSFORM_RULES_KEY) : null;

        if(outputConfigTransformRules==null) throw new ConfigurationException("Processor transformRules property missing or not configured correctly");

        responseTemplateTransform = new ArrayList<>();
        outputConfigTransformRules.forEach(rule -> responseTemplateTransform.add(rule.getString(TRANSFORM_VALUE_KEY)));

        try
        {
            for(int i = 0; i < outputConfigTransformRules.size(); i++)
            {
                transform(input,outputConfigTransformRules.get(i),responseTemplateTransform.get(i),collector,keyStringResolverFactory,i);
            }
        }
        catch (Exception e)
        {
            System.out.println("Exception: " + e.getMessage());
        }
    }

    public void transform(Tuple2<String, ObjectNode> input,Config outputConfig, String responseTransform, Collector<Tuple2<String, ObjectNode>> collector,StringResolverFactory keyStringResolverFactory,int index) throws Exception
    {
        logger.debug("Creating transform rule " + index);
        if (outputConfig.hasPath(RULE_KEY))
        {
            logger.debug("Rule key detected");
            RuleConfiguration ruleConfiguration = new RuleConfiguration(outputConfig.getConfig(RULE_KEY));
            rule = buildRule(ruleConfiguration);

            if(rule.isSatisfiedBy(input.f1))
            {
                logger.debug("Rule satisfied - skipping");
                return;
            }
            else
            {
                logger.debug("Rule not satisfied - processing");
            }
        }
        else if(outputConfig.hasPath(RULES_KEY))
        {
            logger.debug("Rules key detected");

            rules_operator = outputConfig.getString(RULES_OPERATOR_KEY);
            ArrayList<Config> ruleConfigurations = (ArrayList<Config>) outputConfig.getConfigList(RULES_KEY);
            ArrayList<JsonRule> rules = buildRules(ruleConfigurations);
            if(rules_operator.equals("AND"))
            {
                for(int i = 0; i < rules.size(); i++)
                {
                    if(!rules.get(i).isSatisfiedBy(input.f1))
                    {
                        logger.debug("Rules not satisfied for AND - Skipping");
                        return;
                    }
                }
                logger.debug("Rules satisfied for AND - Processing");
            }
            else
            {
                Boolean skip = false;
                for(int i = 0; i < rules.size(); i++)
                {
                    if(rules.get(i).isSatisfiedBy(input.f1))
                    {
                        logger.debug("Rules not satisfied for OR - Skipping");
                        skip = true;
                    }
                }

                if(skip) return;
                logger.debug("Rules satisfied for OR - Processing");
            }
        }
        else
        {
            logger.debug("No rule found.");
        }

        final String key;
        if (outputConfig.hasPathOrNull(TRANSFORM_KEY_KEY))
        {
            if (outputConfig.getIsNull(TRANSFORM_KEY_KEY))
            {
                key = null;
            } else
            {
                final StringSubstitutor keyInputResolver = keyStringResolverFactory.of(input);
                key = keyInputResolver.replace(outputConfig.getString(TRANSFORM_KEY_KEY));
            }
        }
        else
        {
            key = input.f0;
        }

        final StringSubstitutor inputResolver = stringResolverFactory.of(input);
        final String responseBody = inputResolver.replace(responseTransform);

        ObjectNode responseObject = mapper.readTree(responseBody).deepCopy();

        if (outputConfig.hasPath(ADDITIONAL_KEY))
        {
            Config addtionalConfig = outputConfig.getConfig(ADDITIONAL_KEY);

            RuleConfiguration ruleConfiguration = new RuleConfiguration(addtionalConfig.getConfig(RULE_KEY));
            rule = buildRule(ruleConfiguration);
            if(!rule.isSatisfiedBy(input.f1))
            {
                String additionalTransform = addtionalConfig.getString(TRANSFORM_VALUE_KEY);
                final String additionalBody = inputResolver.replace(additionalTransform);
                final ObjectNode additionalObject = mapper.readTree(additionalBody).deepCopy();

                responseObject = (ObjectNode) merge(additionalObject,responseObject);
            }
        }

        Tuple2<String,ObjectNode> result = Tuple2.of(key, responseObject);
        logger.debug("Collecting " + index + ": " + result);
        collector.collect(result);
    }

    public static JsonNode merge(JsonNode mainNode, JsonNode updateNode)
    {
        Iterator<String> fieldNames = updateNode.fieldNames();
        while (fieldNames.hasNext()) {

            String fieldName = fieldNames.next();
            JsonNode jsonNode = mainNode.get(fieldName);
            // if field exists and is an embedded object
            if (jsonNode != null && jsonNode.isObject()) {
                merge(jsonNode, updateNode.get(fieldName));
            }
            else {
                if (mainNode instanceof ObjectNode) {
                    // Overwrite field
                    JsonNode value = updateNode.get(fieldName);
                    ((ObjectNode) mainNode).put(fieldName, value);
                }
            }
        }
        return mainNode;
    }
}

