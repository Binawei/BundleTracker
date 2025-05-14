package bundle.process.enums;

/**
 * Matching behaviour for multiple matches/results/patterns.
 */
public enum MatchingBehaviour {
    ALL,
    ANY,
    NONE;

    public static final MatchingBehaviour DEFAULT = MatchingBehaviour.ANY;
}
