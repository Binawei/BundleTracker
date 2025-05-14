package bundle.process.rules;

import java.io.Serializable;

public interface Rule<T> extends Serializable {
    /**
     * True if the given object satisfies the rule, false otherwise.
     */
    boolean isSatisfiedBy(T object);
}
