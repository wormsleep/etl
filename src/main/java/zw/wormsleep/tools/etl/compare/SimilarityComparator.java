package zw.wormsleep.tools.etl.compare;

/**
 * Created by wormsleep on 2015/11/18.
 * 相似度比较器接口
 */
public interface SimilarityComparator {
    boolean compare(String first, String second);
}
