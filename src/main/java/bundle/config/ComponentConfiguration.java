package bundle.config;

import bundle.exceptions.ConfigurationException;

import java.io.Serializable;
import java.util.Properties;

public interface ComponentConfiguration extends Serializable, Configuration {
    String getName();

    /**
     * Fully qualified class name for component implementation.
     */
    String getClassName();

    /**
     * Additional properties passed to directly to component.
     */
    Properties getProperties();

    /**
     * Get initial parallelism value for component.
     * Return 0 if not set, which implies that the value should not be applied.
     */
    int getParallelism() throws ConfigurationException;

    /**
     * Get maximum parallelism value for component.
     * Returns 0 if not set, which implies that the value should not be applied.
     */
    int getMaxParallelism() throws ConfigurationException; // TODO: move into actual components or factories as well (so that components can override)
}
