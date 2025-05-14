package bundle.config;

import com.typesafe.config.Config;

public class SourceConfiguration extends BaseComponentConfiguration implements ComponentConfiguration {
    public SourceConfiguration(Config config) {
        super(config);
    }
}
