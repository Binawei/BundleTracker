package bundle.process.rules;

import bundle.process.enums.MatchingBehaviour;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class MatchingHelper<P> implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final MatchingBehaviour matchingBehaviour;

    public MatchingHelper(MatchingBehaviour matchingBehaviour) {
        this.matchingBehaviour = matchingBehaviour;
        logger.debug("Matching behaviour: {}", matchingBehaviour);
    }

    public MatchingBehaviour getMatchingBehaviour() {
        return matchingBehaviour;
    }

    public boolean matches(Stream<P> predicateStream, Predicate<P> predicate) {
        final boolean matches;
        switch (getMatchingBehaviour()) {
            case ALL:
                matches = predicateStream.allMatch(predicate);
                break;
            case ANY:
                matches = predicateStream.anyMatch(predicate);
                break;
            case NONE:
                matches = predicateStream.noneMatch(predicate);
                break;
            default:
                // sanity check, should not happen
                throw new RuntimeException(String.format("Matching behaviour '%s' invalid", getMatchingBehaviour()));
        }
        logger.trace("Matches: {}", matches);
        return matches;
    }
}
