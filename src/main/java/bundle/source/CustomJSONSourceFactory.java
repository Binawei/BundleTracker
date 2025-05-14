package bundle.source;

import bundle.config.SourceConfiguration;
import bundle.exceptions.ConfigurationException;
import bundle.exceptions.FactoryException;
import bundle.factory.SourceFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;


public class CustomJSONSourceFactory extends SourceFactory<Tuple2<String, ObjectNode>>
{
    private static final String TYPE = "type"; // optional, will use default if not set
    private static final String DATA = "data"; // optional, will use default if not set
    private static final String WAIT_TIME = "wait_time"; // optional, will use default if not set
    private static final String FILE_PATH = "filePath"; // optional, will use default if not set

    private static final ObjectMapper mapper = new ObjectMapper();

    public CustomJSONSourceFactory(SourceConfiguration configuration) throws FactoryException {
        super(configuration);
    }

    @Override
    public SourceFunction<Tuple2<String, ObjectNode>> create() throws ConfigurationException
    {
        final SourceConfiguration configuration = getConfiguration();
        final Config config = configuration.getConfig();

        String type = config.hasPath(TYPE) ? config.getString(TYPE) : null;
        String data = config.hasPath(DATA) ? config.getString(DATA) : null;
        String filePath = config.hasPath(FILE_PATH) ? config.getString(FILE_PATH) : null;

        long waitTime = config.hasPath(WAIT_TIME) ? config.getLong(WAIT_TIME) : 1000L;

        SourceFunction sourceFunction = null;
        try
        {
            if(type.equals("test"))
            {
                sourceFunction = new TestScenarioJsonObjectSourceFunction(filePath);
            }
            else
            {
                sourceFunction = new JsonObjectSourceFunction(type, "key", mapper.readValue(data, JsonNode.class), waitTime);
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return sourceFunction;
    }

    @Override
    public TypeHint<Tuple2<String, ObjectNode>> returns() {
        return new TypeHint<Tuple2<String, ObjectNode>>() {
        };
    }
}
