package bundle.factory;

import bundle.config.Configuration;
import bundle.exceptions.FactoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;

/**
 * Base factory for all component types.
 */
public abstract class ComponentFactory {
    private static final Logger logger = LoggerFactory.getLogger(ComponentFactory.class);

    /**
     * Dynamically construct a component class using reflection.
     * @param className fully qualified name of class to construct
     * @param configuration configuration for component
     * @return constructed component class instance
     */
    protected static Object construct(String className, Configuration configuration) throws FactoryException {
        final String configurationClassName = configuration.getClass().getName();
        logger.debug("Constructing class '{}' with configuration class '{}'", className, configurationClassName);
        try {
            Class<?> clazz = Class.forName(className);
            Class<?> configurationClass = configuration.getClass();
            Constructor<?> constructor = clazz.getConstructor(configurationClass);

            Object object = constructor.newInstance(configuration);

            logger.debug("Constructed: {}", object);
            return object;
        } catch(ClassNotFoundException exception) {
            final String message = String.format("Cannot find class with name '%s'", className);
            logger.error(message);
            throw new FactoryException(message, exception);
        } catch(NoSuchMethodException exception) {
            final String message = String.format("Class with name '%s' missing constructor with configuration class '%s'",
                    className, configurationClassName);
            logger.error(message);
            throw new FactoryException(message, exception);
        } catch (ReflectiveOperationException exception) {
            final String message = String.format("Error constructing class with name '%s'", className);
            logger.error(message);
            throw new FactoryException(message, exception);
        }
    }
}
