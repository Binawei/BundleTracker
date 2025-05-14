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

public class HighExcessiveChange extends GoalIdAllocation {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final List<String> excessiveNotificationStages;
    private final List<Map<String, String>> bundleTypes;

    public HighExcessiveChange(ObjectNode inputNode, List<Map<String, String>> bundleTypes, List<String> excessiveNotificationStages) {
        super(inputNode);
        this.excessiveNotificationStages = excessiveNotificationStages;
        this.bundleTypes = bundleTypes;
    }

    public void processHighExcessiveChange(Tuple2<String, ObjectNode> payload, ProcessFunction<Tuple2<String, ObjectNode>, Tuple2<String, ObjectNode>>.Context context, Collector<Tuple2<String, ObjectNode>> collector) throws Exception {
        logger.info("processing High Excessive Change type event {}", payload.f1);
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

        if (active != null && active.equals("1")) {
            String highUsageAlert = selectorValue(inputNode, "HIGH_USAGE_ALERT_NOTIFICATION_IND");
            if ("1".equals(highUsageAlert)) {
                addFieldToPayload(inputNode, "OptinType", OptinType.OPTIN.toString());
                formulateGoalID(inputNode, payload, collector, bundleType, "HIGH_USAGE_STAGE", allocationField);
            } else if ("0".equals(highUsageAlert)) {
                addFieldToPayload(inputNode, "OptinType", OptinType.OPTOUT.toString());
                formulateGoalID(inputNode, payload, collector, bundleType, "HIGH_USAGE_STAGE", allocationField);
            }

            String excessiveUsageAlert = selectorValue(inputNode, "EXCESSIVE_USAGE_ALERT_NOTIFICATION_IND");
            if ("1".equals(excessiveUsageAlert)) {
                addFieldToPayload(inputNode, "OptinType", OptinType.OPTIN.toString());
                getExcessiveGoalIdAllocation(inputNode, payload, collector, bundleType, allocationField, excessiveNotificationStages);
            } else if ("0".equals(excessiveUsageAlert)) {
                addFieldToPayload(inputNode, "OptinType", OptinType.OPTOUT.toString());
                getExcessiveGoalIdAllocation(inputNode, payload, collector, bundleType, allocationField, excessiveNotificationStages);
            }
        }
    }
}
