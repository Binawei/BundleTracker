package bundle.config;

import com.typesafe.config.Config;

import java.io.Serializable;
import java.util.List;

/**
 * Wrapper around {@link Config} to allow for specifying default values.
 */
public class ConfigWrapper implements Serializable {
    private static final long serialVersionUID = 1L;
    private final Config config;

    public ConfigWrapper(Config config) {
        this.config = config;
    }

    public boolean hasPath(String path) {
        return getConfig().hasPath(path);
    }

    public boolean getBoolean(String path, boolean defaultValue) {
        if (!hasPath(path)) {
            return defaultValue;
        }
        return getConfig().getBoolean(path);
    }

    public String getString(String path, String defaultValue) {
        if (!hasPath(path)) {
            return defaultValue;
        }
        return getConfig().getString(path);
    }

    /**
     * Get an {@link Integer} value for a given path (can be defaulted to null).
     */
    public Integer getInteger(String path, Integer defaultValue) {
        if (!hasPath(path)) {
            return defaultValue;
        }
        return getConfig().getInt(path);
    }

    /**
     * Get an integer value for a given path (can not be defaulted to null).
     */
    public int getInt(String path, int defaultValue) {
        if (!hasPath(path)) {
            return defaultValue;
        }
        return getConfig().getInt(path);
    }

    /**
     * Get a {@link Long} value for a given path (can be defaulted to null).
     */
    public Long getLong(String path, Long defaultValue) {
        if (!hasPath(path)) {
            return defaultValue;
        }
        return getConfig().getLong(path);
    }

    /**
     * Get a long value for a given path (can not be defaulted to null).
     */
    public long getLong(String path, long defaultValue) {
        if (!hasPath(path)) {
            return defaultValue;
        }
        return getConfig().getLong(path);
    }

    public List<Config> getConfigList(String path, List<Config> defaultValue) {
        if (!hasPath(path)) {
            return defaultValue;
        }
        return (List<Config>) getConfig().getConfigList(path);
    }

    public Config getConfig() {
        return config;
    }
}
