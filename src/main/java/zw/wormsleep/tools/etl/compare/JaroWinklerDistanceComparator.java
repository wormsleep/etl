package zw.wormsleep.tools.etl.compare;

import org.apache.commons.lang3.StringUtils;

/**
 * Created by wormsleep on 2015/11/18.
 * JaroWinklerDistance 相似度比较器
 */
public class JaroWinklerDistanceComparator implements SimilarityComparator {

    private Double threshold;

    public JaroWinklerDistanceComparator(Double threshold) {
        this.threshold = threshold;
    }

    @Override
    public boolean compare(String first, String second) {
        return threshold.compareTo(StringUtils.getJaroWinklerDistance(first, second)) <= 0 ? true : false;
    }
}
