package bundle.process;

import bundle.config.OperatorConfiguration;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base for processor operators that can transform values.
 * @param <S> source type
 * @param <D> destination type
 */
public abstract class ProcessOperator<S, D> extends ProcessFunction<S, D> implements OperatorFunction<S, D> {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final OperatorConfiguration.ProcessOperatorConfiguration configuration;

    protected ProcessOperator(OperatorConfiguration.ProcessOperatorConfiguration configuration) {
        this.configuration = configuration;
    }

    public abstract void processElement(Tuple2<String, ObjectNode> input, Context context, Collector<Tuple2<String, ObjectNode>> collector) throws Exception;

    public abstract TypeHint<D> returns();

    protected abstract void processElementInternal(S input, Context context, Collector<D> collector) throws Exception;

    @Override
    public void processElement(S input, Context context, Collector<D> collector) throws Exception {
        try {
            processElementInternal(input, context, collector);
        } catch (Exception exception) {
            logger.error("Error processing element", exception);
            if (getOperatorConfiguration().getErrorHandlingConfiguration().shouldIgnoreErrors()) {
                logger.warn("Ignoring error");
                return;
            }
            throw exception;
        }
    }

    public OperatorConfiguration.ProcessOperatorConfiguration getOperatorConfiguration() {
        return configuration;
    }
}
