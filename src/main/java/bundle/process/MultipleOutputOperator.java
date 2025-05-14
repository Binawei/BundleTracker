package bundle.process;

import bundle.config.OperatorConfiguration;
import bundle.process.ruleType.GoalIdAllocation;
import bundle.process.ruleType.HighExcessiveChange;
import bundle.process.ruleType.NewOptin;
import bundle.process.ruleType.ProfileChange;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class MultipleOutputOperator extends JsonProcessOperator {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final List<String> notificationStages;
    private final List<String> excessiveNotificationStages;
    private final List<Map<String, String>> bundleTypes;


    public MultipleOutputOperator(OperatorConfiguration.ProcessOperatorConfiguration configuration) {
        super(configuration);
        Config config = getOperatorConfiguration().getConfig();
        notificationStages = config.getStringList("notificationStages");
        excessiveNotificationStages = config.getStringList("excessivenotificationStages");
        bundleTypes = (List<Map<String, String>>) config.getAnyRefList("bundleTypes");
    }

    @Override
    protected void processElementInternal(Tuple2<String, ObjectNode> payload, ProcessFunction<Tuple2<String, ObjectNode>, Tuple2<String, ObjectNode>>.Context context, Collector<Tuple2<String, ObjectNode>> collector) throws Exception {
        try {
            logger.info("processElementInternal for MultipleOutputOperator with received event {}", payload.f1);
            ObjectNode inputNode = payload.f1;
            if (inputNode == null) {
                logger.error("InputNode is null");
                return;
            }
            if (inputNode.get("Event") == null) {
                logger.error("Event field is missing in inputNode");
                return;
            }

            String eventType = inputNode.get("Event").asText().toLowerCase();
            String externalId = "EVS" + UUID.randomUUID();
            addFieldToPayload(inputNode, "externalId", externalId);
            logger.info("Received event is :  {}", inputNode);
            logger.info("Received event is of type :  {}", eventType);
            GoalIdAllocation Event = null;
            switch (eventType) {
                case "new_optin":
                    Event = new NewOptin(inputNode, bundleTypes, notificationStages, excessiveNotificationStages);
                    ((NewOptin) Event).processOptin(payload, context, collector);
                    break;
                case "profile_change":
                    Event = new ProfileChange(inputNode, bundleTypes, notificationStages, excessiveNotificationStages);
                    ((ProfileChange) Event).processProfileChange(payload, context, collector);
                    break;
                case "high_excessive_change":
                    Event = new HighExcessiveChange(inputNode, bundleTypes, excessiveNotificationStages);
                    ((HighExcessiveChange) Event).processHighExcessiveChange(payload, context, collector);
                    break;
                case "reassign":
                    Event = new ProfileChange(inputNode, bundleTypes, notificationStages, excessiveNotificationStages);
                    ((ProfileChange) Event).processProfileChange(payload, context, collector);
                    break;
                default:
                    addFieldToPayload(inputNode, "alert", "Y");
                    addFieldToPayload(inputNode, "alertType", "ERROR");
                    logger.debug("Event is for deactivated Account: {}", eventType);
                    collector.collect(Tuple2.of(payload.f0, inputNode));
            }
        } catch (Exception e) {
            logger.error("Error processing element: ", e);
            addFieldToPayload(payload.f1, "alert", "Y");
            addFieldToPayload(payload.f1, "alertType", "ERROR");
            addFieldToPayload(payload.f1, "errorMessage", e.getMessage());
        }
    }



    private void addFieldToPayload(ObjectNode payload, String fieldName, String value) {
        payload.put(fieldName, value);
    }

}