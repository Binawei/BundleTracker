package bundle.source;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.nio.file.Paths;
import java.util.Iterator;

public class TestScenarioJsonObjectSourceFunction implements SourceFunction<Tuple2<String, ObjectNode>> {

    private static String filePath;
    public TestScenarioJsonObjectSourceFunction(String filePathTest)
    {
        filePath = filePathTest;
    }

    @Override
    public void run(SourceContext<Tuple2<String, ObjectNode>> sourceContext) throws Exception
    {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode file = mapper.readValue(Paths.get(filePath).toFile(),ObjectNode.class);
        ArrayNode testScenarios = (ArrayNode) file.get("testScenarios");

        Iterator it = testScenarios.iterator();

        while(it.hasNext())
        {
            ObjectNode scenario = (ObjectNode) it.next();
            ObjectNode input = (ObjectNode) scenario.get("input");
            System.out.println(input);
            sourceContext.collect(new Tuple2<String,ObjectNode>("key",input));
            Thread.sleep(15000);
        }
    }


    @Override
    public void cancel() {

    }
}