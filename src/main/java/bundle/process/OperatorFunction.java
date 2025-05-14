package bundle.process;


import bundle.config.OperatorConfiguration;

import java.io.Serializable;

/**
 * Base for all operators.
 * @param <S> source type
 * @param <D> destination type
 */
public interface OperatorFunction<S, D> extends Serializable {
    OperatorConfiguration getOperatorConfiguration();
}
