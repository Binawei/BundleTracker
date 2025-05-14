package bundle.helpers;

import bundle.config.OperatorConfiguration;
import bundle.exceptions.ConfigurationException;
import bundle.process.ProcessOperator;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Base class for process operators that accept and return string-keyed JSON objects.
 */
public abstract class JsonObjectProcessOperator extends ProcessOperator<Tuple2<String, ObjectNode>, Tuple2<String, ObjectNode>> {
    protected JsonObjectProcessOperator(OperatorConfiguration.ProcessOperatorConfiguration configuration) throws ConfigurationException {
        super(configuration);
    }

    @Override
    public TypeHint<Tuple2<String, ObjectNode>> returns() {
        return new TypeHint<Tuple2<String, ObjectNode>>() {
        };
    }

    protected abstract void processElementInternal(Tuple2<String, ObjectNode> input, ProcessFunction<Tuple2<String, ObjectNode>, Tuple2<String, ObjectNode>>.Context context, Collector<Tuple2<String, ObjectNode>> collector) throws Exception;

}
