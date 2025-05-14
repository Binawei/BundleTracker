package bundle.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Iterator;

public class JsonObjectSourceFunction<OUT> implements SourceFunction<Tuple2<String,ObjectNode>>
{
    private static Boolean isRunning = true;
    private static JsonNode message;
    private static long waitTime;
    private static String key;
    private static String type;


    public JsonObjectSourceFunction(String type, String key, JsonNode message, long waitTime)
    {
        this.message = message;
        this.waitTime = waitTime;
        this.key = key;
        this.type = type;

    }

    @Override
    public void run(SourceContext<Tuple2<String,ObjectNode>> sourceContext) throws Exception
    {
        if(type.equals("object")) generateObject(sourceContext);
        else generateArray(sourceContext);

    }

    public static void generateObject(SourceContext<Tuple2<String,ObjectNode>> sourceContext)
    {
        try
        {
            while(isRunning)
            {
                sourceContext.collect(new Tuple2(key,message));
                Thread.sleep(waitTime);
            }
        }
        catch (Exception e)
        {
            isRunning = false;
            System.out.println(e.getMessage());
        }
    }

    public static void generateArray(SourceContext<Tuple2<String,ObjectNode>> sourceContext)
    {
        try
        {
            ArrayNode arrayNode = (ArrayNode) message;

            while(isRunning)
            {

                Iterator iterator = arrayNode.iterator();

                while(iterator.hasNext())
                {
                    sourceContext.collect(new Tuple2(key,iterator.next()));
                }
                Thread.sleep(waitTime);
            }
        }
        catch (Exception e)
        {
            isRunning = false;
            System.out.println(e.getMessage());
        }
    }



    @Override
    public void cancel() {

    }
}




