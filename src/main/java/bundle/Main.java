package bundle;

import bundle.config.ConfigurationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    public static void main(String[] args) throws Exception {
        System.out.println("Hello world!");
        ConfigurationManager configurationManager = ConfigurationManager.load(args);
        logger.info("Creating streaming application from configuration for job with name: {}", configurationManager.getJobName());

        StreamingApplication application = new StreamingApplication(configurationManager);
        application.run();
    }
}