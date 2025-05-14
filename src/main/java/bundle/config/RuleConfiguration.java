package bundle.config;

import com.typesafe.config.Config;

public class RuleConfiguration implements Configuration {
    private static final long serialVersionUID = 1L;

    private final Config config;

    public RuleConfiguration(Config config) {
        this.config = config;
    }

    public Config getConfig() {
        return config;
    }
}
