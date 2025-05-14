package bundle.process.rules;

import bundle.config.RuleConfiguration;
import bundle.process.enums.MatchingBehaviour;
import com.fasterxml.jackson.databind.JsonNode;
import com.typesafe.config.Config;

import java.util.function.Predicate;
import java.util.stream.Stream;

public abstract class JsonMatchingRule<P> extends JsonSelectorRule {

    private static final String MATCHING_KEY = "matching";

    private final MatchingHelper<P> matchingHelper;

    protected JsonMatchingRule(RuleConfiguration configuration) {
        super(configuration);

        final Config config = getConfiguration().getConfig();

        final MatchingBehaviour matchingBehaviour = config.hasPath(MATCHING_KEY) ? config.getEnum(MatchingBehaviour.class, MATCHING_KEY) : MatchingBehaviour.DEFAULT;
        matchingHelper = new MatchingHelper<>(matchingBehaviour);
    }

    protected MatchingBehaviour getMatchingBehaviour() {
        return getMatchingHelper().getMatchingBehaviour();
    }

    protected MatchingHelper<P> getMatchingHelper() {
        return matchingHelper;
    }

    @Override
    protected boolean isSatisfiedBySelectedNode(JsonNode node) {
        final Stream<P> predicateStream = getPredicateStream(node);
        final Predicate<P> predicate = getPredicate(node);

        return getMatchingHelper().matches(predicateStream, predicate);
    }

    protected abstract Predicate<P> getPredicate(JsonNode node);

    protected abstract Stream<P> getPredicateStream(JsonNode node);
}
