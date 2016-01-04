package zw.wormsleep.tools.etl.compare;

import org.apache.commons.lang3.StringUtils;

/**
 * Created by wormsleep on 2015/11/18.
 * JaroWinklerDistance 相似度比较器
 */
public class JaroWinklerDistanceComparator implements SimilarityComparator {

    private final Double MAX_THRESHOLD = new Double("1.0");
    private Double threshold;

    public JaroWinklerDistanceComparator(Double threshold) {
        this.threshold = threshold;
    }

    @Override
    public boolean compare(String first, String second) {
        if(threshold.compareTo(MAX_THRESHOLD) == 0 || (first.length() < 5 && second.length() < 5) ) {
            return first.equalsIgnoreCase(second);
        } else {
            if (Math.abs(first.length() - second.length()) > 4) {
                return false;
            } else {
                return threshold.compareTo(StringUtils.getJaroWinklerDistance(first, second)) <= 0 ? true : false;
            }
        }
    }

    public boolean compareOrigin(String first, String second) {
        return (threshold.compareTo(MAX_THRESHOLD) == 0) ? (first.equalsIgnoreCase(second)) : (threshold.compareTo(StringUtils.getJaroWinklerDistance(first, second)) <= 0 ? true : false);
    }

}
