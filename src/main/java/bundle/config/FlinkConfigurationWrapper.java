package bundle.config;

import bundle.helpers.ConfigurationHelper;
import com.typesafe.config.Config;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;

import java.util.Map;

/**
 * Wrapper around {@link Configuration}.
 * Adds support for instantiation from other configuration objects.
 */
public class FlinkConfigurationWrapper extends Configuration implements ReadableConfig {

    @SuppressWarnings("unused")
    protected FlinkConfigurationWrapper() {
        super();
    }

    @SuppressWarnings("unused")
    protected FlinkConfigurationWrapper(Configuration other) {
        super(other);
    }

    public static ReadableConfig of(Map<String, Object> map) {
        FlinkConfigurationWrapper configuration = new FlinkConfigurationWrapper();
        configuration.confData.putAll(map);
        return configuration;
    }

    public static ReadableConfig of(Config config) {
        return of(ConfigurationHelper.toMap(config));
    }
}
