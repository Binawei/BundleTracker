package bundle.process.ruleType;
import bundle.process.enums.OptinType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class NewOptin extends GoalIdAllocation {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final List<String> notificationStages;
    private final List<String> excessiveNotificationStages;
    private final List<Map<String, String>> bundleTypes;

    public NewOptin(ObjectNode inputNode, List<Map<String, String>> bundleTypes, List<String> notificationStages, List<String> excessiveNotificationStages) {
        super(inputNode);
        this.notificationStages = notificationStages;
        this.excessiveNotificationStages = excessiveNotificationStages;
        this.bundleTypes = bundleTypes;
    }

    public void processOptin(Tuple2<String, ObjectNode> payload, ProcessFunction<Tuple2<String, ObjectNode>, Tuple2<String, ObjectNode>>.Context context, Collector<Tuple2<String, ObjectNode>> collector) throws Exception {
        logger.info("processing New-Optin type event {}", payload.f1);
        ObjectNode inputNode = payload.f1;
        for (Map<String, String> dataType : bundleTypes) {
            addFieldToPayload(inputNode, "OptinType", OptinType.NewOptIn.toString());
            String activeInd = dataType.get("activeInd");
            String type = dataType.get("type");
            String allocationField = dataType.get("allocationField");
            processDataType(inputNode, payload, collector, activeInd, type, allocationField);
        }
    }

    public void handleNewOptIn(ObjectNode inputNode, Tuple2<String, ObjectNode> payload, Collector<Tuple2<String, ObjectNode>> collector,
                            String bundleType, String allocationField,
                            List<String> excessiveNotificationStages,List<String> notificationStages) {
        addFieldToPayload(inputNode, "OptinType", OptinType.NewOptIn.toString());
        getGoalIdAllocation(inputNode, payload, collector, bundleType, allocationField, notificationStages);

        if ("1".equals(selectorValue(inputNode, "HIGH_USAGE_ALERT_NOTIFICATION_IND"))) {
            formulateGoalID(inputNode, payload, collector, bundleType, "HIGH_USAGE_STAGE", allocationField);
        }
        if ("1".equals(selectorValue(inputNode, "EXCESSIVE_USAGE_ALERT_NOTIFICATION_IND"))) {
            getExcessiveGoalIdAllocation(inputNode, payload, collector, bundleType, allocationField, excessiveNotificationStages);
        }
    }

    private void processDataType(ObjectNode inputNode, Tuple2<String, ObjectNode> payload, Collector<Tuple2<String, ObjectNode>> collector, String activeInd, String bundleType, String allocationField) {
        String active = selectorValue(inputNode, activeInd);

        if (active != null && !active.equals("0")) {
            logger.info("{} is active", bundleType);
            handleNewOptIn(inputNode, payload, collector, bundleType, allocationField, excessiveNotificationStages, notificationStages);
        }
    }
}
