package bundle.config;

import com.typesafe.config.Config;

import java.io.Serializable;

public interface Configuration extends Serializable {
    Config getConfig();
}
