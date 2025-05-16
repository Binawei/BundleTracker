package bundle;

import bundle.config.ConfigurationManager;
import bundle.config.OperatorConfiguration;
import bundle.config.SinkConfiguration;
import bundle.config.SourceConfiguration;
import bundle.exceptions.ConfigurationException;
import bundle.exceptions.FactoryException;
import bundle.factory.OperatorFactory;
import bundle.factory.SinkFactory;
import bundle.factory.SourceFactory;
import bundle.process.OperatorFunction;
import bundle.process.ProcessOperator;
import bundle.sinks.BaseSinkFunction;
import bundle.sinks.SinkFunctionWrapper;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

@SuppressWarnings({"rawtypes", "unchecked"})
public class StreamingApplication {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final ConfigurationManager configurationManager;
    private StreamExecutionEnvironment env = null;
    private boolean initialized = false;

    public StreamingApplication(ConfigurationManager configurationManager) {
        this.configurationManager = configurationManager;
    }

    public void initialize() throws FactoryException, ConfigurationException {
        if (initialized) {
            throw new RuntimeException("Application must only be initialized once");
        }

        // set up the streaming execution environment
        if (configurationManager.getIsLocal()) {
            final int parallelism = StreamExecutionEnvironment.getDefaultLocalParallelism();
            final Configuration envConfiguration = configurationManager.getEnvironmentConfiguration();
            env = StreamExecutionEnvironment.createLocalEnvironment(parallelism, envConfiguration);
        } else {
            // does not initialise the configuration from `flink-conf.yaml` when running in standalone mode
            env = StreamExecutionEnvironment.getExecutionEnvironment();
        }

        // set up the checkpoint environment
        if (configurationManager.hasEnvConfig()) {
            env.getCheckpointConfig().configure(configurationManager.getEnvConfiguration());
        }

        // TODO: proper generic type handling for source
        SourceConfiguration sourceConfiguration = configurationManager.getSourceConfiguration();
        SourceFactory sourceFactory = SourceFactory.get(sourceConfiguration);
        SourceFunction<Object> source = sourceFactory.create();

        SingleOutputStreamOperator sourceStream = env
                .addSource(source)
                .returns(sourceFactory.returns());
        if (sourceConfiguration.getParallelism() > 0) {
            sourceStream = sourceStream.setParallelism(sourceConfiguration.getParallelism());
        }
        if (sourceConfiguration.getMaxParallelism() > 0) {
            sourceStream = sourceStream.setMaxParallelism(sourceConfiguration.getMaxParallelism());
        }
        DataStream<Object> stream = sourceStream.name(sourceConfiguration.getName());

        // TODO: proper generic type handling
        List<OperatorConfiguration> operatorConfigurations = configurationManager.getOperatorConfigurations();
        for (OperatorConfiguration operatorConfiguration : operatorConfigurations) {
            final String name = operatorConfiguration.getName();
            OperatorFactory<Object, Object> factory = OperatorFactory.get(operatorConfiguration);
            OperatorFunction<Object, Object> operator = factory.create();
            SingleOutputStreamOperator<Object> singleOutputStreamOperator;
            if (operator instanceof FilterFunction) {
                FilterFunction<Object> filterFunction = (FilterFunction<Object>) operator;
                singleOutputStreamOperator = stream
                        .filter(filterFunction);
            } else if (operator instanceof ProcessOperator) {
                ProcessOperator processOperator = (ProcessOperator) operator;
                TypeHint returns = (processOperator).returns();
                singleOutputStreamOperator = stream
                        .process(processOperator)
                        .returns(returns);
            } else {
                final String errorMessage = String.format("Invalid operator with class '%s'", operator.getClass().getName());
                logger.error(errorMessage);
                throw new ConfigurationException(errorMessage);
            }
            if (operatorConfiguration.getParallelism() > 0) {
                singleOutputStreamOperator = singleOutputStreamOperator.setParallelism(operatorConfiguration.getParallelism());
            }
            if (operatorConfiguration.getMaxParallelism() > 0) {
                singleOutputStreamOperator = singleOutputStreamOperator.setMaxParallelism(operatorConfiguration.getMaxParallelism());
            }
            stream = singleOutputStreamOperator.name(name);
        }

        // TODO: proper generic type handling for sink
        SinkConfiguration sinkConfiguration = configurationManager.getSinkConfiguration();
        SinkFactory sinkFactory = SinkFactory.get(sinkConfiguration);
        SinkFunction sink = sinkFactory.create();
        if (!(sink instanceof BaseSinkFunction)) {
            // sink function will not support output metrics, so consider wrapping
            if (sinkConfiguration.shouldAllowWrapping()) {
                // wrap sink function for metrics recording
                logger.info("Wrapping sink function for component '{}' with class: {}",
                        sinkConfiguration.getName(), sinkConfiguration.getClassName());
                sink = new SinkFunctionWrapper(sinkConfiguration, sink);
            } else {
                logger.info("Not allowed to wrap sink function for component '{}' with class: {}",
                        sinkConfiguration.getName(), sinkConfiguration.getClassName());
            }
        }

        DataStreamSink streamSink = stream.addSink(sink);
        if (sinkConfiguration.getParallelism() > 0) {
            streamSink = streamSink.setParallelism(sinkConfiguration.getParallelism());
        }
        streamSink.name(sinkConfiguration.getName());

        String executionPlan = env.getExecutionPlan();
        logger.info("Execution plan: {}", executionPlan);

        initialized = true;
    }

    public void run() throws Exception {
        if (!initialized) {
            initialize();
        }

        // execute program
        final String jobName = configurationManager.getJobName();
        logger.info("Executing job: {}", jobName);
        env.execute(jobName);
    }
}
