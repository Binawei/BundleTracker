package bundle.process;

import bundle.config.OperatorConfiguration;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class EventTimeOperator extends JsonProcessOperator {
    Logger logger = LoggerFactory.getLogger(getClass());

    public EventTimeOperator(OperatorConfiguration.ProcessOperatorConfiguration configuration) {
        super(configuration);
    }

    @Override
    protected void processElementInternal(Tuple2<String, ObjectNode> input, ProcessFunction<Tuple2<String, ObjectNode>, Tuple2<String, ObjectNode>>.Context context, Collector<Tuple2<String, ObjectNode>> collector) throws Exception {
        logger.debug("Event time and process operator");

        ObjectNode payload = input.f1;

        long eventStartTimeMillis = System.currentTimeMillis();
        String formattedEventStartTime = getFormattedTime(eventStartTimeMillis);
        addFieldToPayload(payload, "eventStartTime", formattedEventStartTime);
        logger.info("eventStartTime field value {}", formattedEventStartTime);

        long eventEndTimeMillis = eventStartTimeMillis + (30L * 24 * 60 * 60 * 1000);
        String formattedEventEndTime = getFormattedTime(eventEndTimeMillis);
        addFieldToPayload(payload, "eventEndTime", formattedEventEndTime);
        logger.info("eventEndTime field value {}", formattedEventEndTime);

        collector.collect(Tuple2.of(input.f0, payload));
    }

    private void addFieldToPayload(ObjectNode payload, String fieldName, String value) {
        payload.put(fieldName, value);
    }

    private String getFormattedTime(long timeMillis) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                .withZone(ZoneId.of("UTC"));
        return formatter.format(Instant.ofEpochMilli(timeMillis));
    }
}
