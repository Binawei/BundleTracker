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

public class ProfileChange extends GoalIdAllocation{
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final List<String> notificationStages;
    private final List<String> excessiveNotificationStages;
    private final List<Map<String, String>> bundleTypes;

    public ProfileChange(ObjectNode inputNode, List<Map<String, String>> bundleTypes, List<String> notificationStages, List<String> excessiveNotificationStages) {
        super(inputNode);
        this.notificationStages = notificationStages;
        this.excessiveNotificationStages = excessiveNotificationStages;
        this.bundleTypes = bundleTypes;
    }

    public void processProfileChange(Tuple2<String, ObjectNode> payload, ProcessFunction<Tuple2<String, ObjectNode>, Tuple2<String, ObjectNode>>.Context context, Collector<Tuple2<String, ObjectNode>> collector) throws Exception {
        logger.info("processing event {}", payload.f1);
        ObjectNode inputNode = payload.f1;

        for (Map<String, String> bundletype : bundleTypes) {
            String activeInd = bundletype.get("activeInd");
            String type = bundletype.get("type");
            String allocationField = bundletype.get("allocationField");
            processDataType(inputNode, payload, collector, activeInd, type, allocationField);
        }
    }

    private void processDataType(ObjectNode inputNode, Tuple2<String, ObjectNode> payload, Collector<Tuple2<String, ObjectNode>> collector, String activeInd, String bundleType, String allocationField) {
        String active = selectorValue(inputNode, activeInd);

        if (active != null) {
            if (active.equals("1")) {
                handleOptIn(inputNode, payload, collector, bundleType, allocationField, excessiveNotificationStages, notificationStages);
            } else if (active.equals("0")) {
                handleOptOut(inputNode, payload, collector, bundleType, allocationField);
            } else {
                logger.info("{} is not active (OptOut) or (Optin)", active);
            }
            if (active.equals("1") && "0".equals(selectorValue(inputNode, "HIGH_USAGE_ALERT_NOTIFICATION_IND"))) {
                addFieldToPayload(inputNode, "OptinType", OptinType.OPTOUT.toString());
                formulateGoalID(inputNode, payload, collector, bundleType, "HIGH_USAGE_STAGE", allocationField);
            }

            if (active.equals("1") && "0".equals(selectorValue(inputNode, "EXCESSIVE_USAGE_ALERT_NOTIFICATION_IND"))) {
                addFieldToPayload(inputNode, "OptinType", OptinType.OPTOUT.toString());
                getExcessiveGoalIdAllocation(inputNode, payload, collector, bundleType, allocationField, excessiveNotificationStages);
            }

        }
    }

    private void handleOptOut(ObjectNode inputNode, Tuple2<String, ObjectNode> payload, Collector<Tuple2<String, ObjectNode>> collector, String bundleType, String allocationField) {
        logger.info("{} is inactive (OptOut)", bundleType);
        addFieldToPayload(inputNode, "OptinType", OptinType.OPTOUT.toString());
        getGoalIdAllocation(inputNode, payload, collector, bundleType, allocationField, notificationStages);

        if ("1".equals(selectorValue(inputNode, "HIGH_USAGE_ALERT_NOTIFICATION_IND"))) {
            formulateGoalID(inputNode, payload, collector, bundleType, "HIGH_USAGE_STAGE", allocationField);
        }
        if ("1".equals(selectorValue(inputNode, "EXCESSIVE_USAGE_ALERT_NOTIFICATION_IND"))) {
            getExcessiveGoalIdAllocation(inputNode, payload, collector, bundleType, allocationField, excessiveNotificationStages);
        }
    }

}
