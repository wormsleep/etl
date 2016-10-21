package zw.wormsleep.tools.etl.compare;

import org.apache.commons.lang3.StringUtils;

/**
 * Created by wormsleep on 2015/12/30.
 */
public class LevenshteinDistanceComparator implements SimilarityComparator {
    private int threshold;

    public LevenshteinDistanceComparator(int threshold) {
        this.threshold = threshold;
    }

    @Override
    public boolean compare(String first, String second) {
        return (threshold == 0) ? (first.equalsIgnoreCase(second)) : (StringUtils.getLevenshteinDistance(first, second, threshold) < 0 ? true : false);
    }
}
