package bundle.process.ruleType;

import bundle.process.JsonSelector;
import bundle.process.enums.OptinType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.List;

public abstract class GoalIdAllocation {
    protected final ObjectNode inputNode;

    protected GoalIdAllocation(ObjectNode inputNode) {
        this.inputNode = inputNode;
    }

    public void formulateGoalID(ObjectNode inputNode, Tuple2<String, ObjectNode> payload,
                                Collector<Tuple2<String, ObjectNode>> collector, String bundleType,
                                String stage, String allocationField) {
        String goalId = "C_" + bundleType + selectorValue(inputNode, stage);
        addFieldToPayload(inputNode, "goalId", goalId);
        addFieldToPayload(inputNode, "allocation", selectorValue(inputNode, allocationField));
        collector.collect(Tuple2.of(payload.f0, inputNode));
    }

    public void addFieldToPayload(ObjectNode payload, String fieldName, String value) {
        payload.put(fieldName, value);
    }

    public String selectorValue(ObjectNode payload, String fieldName) {
        return JsonSelector.parse("/" + fieldName).select(payload).asText();
    }

    public void getGoalIdAllocation(ObjectNode inputNode, Tuple2<String, ObjectNode> payload, Collector<Tuple2<String, ObjectNode>> collector,
                                    String bundleType, String allocationField, List<String> notificationStages) {
        for (String stage : notificationStages) {
            formulateGoalID(inputNode, payload, collector, bundleType, stage, allocationField);
        }
    }

    public void getExcessiveGoalIdAllocation(ObjectNode inputNode, Tuple2<String, ObjectNode> payload,
                                             Collector<Tuple2<String, ObjectNode>> collector, String bundleType, String allocationField, List<String> excessiveNotificationStages) {
        for (String stage : excessiveNotificationStages) {
            formulateGoalID(inputNode, payload, collector, bundleType, stage, allocationField);
        }
    }

    public void handleOptIn(ObjectNode inputNode, Tuple2<String, ObjectNode> payload, Collector<Tuple2<String, ObjectNode>> collector,
                            String bundleType, String allocationField,
                             List<String> excessiveNotificationStages,List<String> notificationStages) {
        addFieldToPayload(inputNode, "OptinType", OptinType.OPTIN.toString());
        getGoalIdAllocation(inputNode, payload, collector, bundleType, allocationField, notificationStages);

        if ("1".equals(selectorValue(inputNode, "HIGH_USAGE_ALERT_NOTIFICATION_IND"))) {
            formulateGoalID(inputNode, payload, collector, bundleType, "HIGH_USAGE_STAGE", allocationField);
        }
        if ("1".equals(selectorValue(inputNode, "EXCESSIVE_USAGE_ALERT_NOTIFICATION_IND"))) {
            getExcessiveGoalIdAllocation(inputNode, payload, collector, bundleType, allocationField, excessiveNotificationStages);
        }
    }
}
