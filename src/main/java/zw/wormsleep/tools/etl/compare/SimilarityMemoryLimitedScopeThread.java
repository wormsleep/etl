package zw.wormsleep.tools.etl.compare;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wormsleep on 2015/12/29.
 */
public class SimilarityMemoryLimitedScopeThread extends Thread {
    final Logger logger = LoggerFactory.getLogger(SimilarityMemoryLimitedScopeThread.class);

    private final String DEFAULT_SEPARATOR = "\t";
    private final String DEFAULT_ENCODING = "UTF-8";

    private File f;
    private String fSeparator;
    private String fEncoding;
    private File s;
    private String sSeparator;
    private String sEncoding;
    private Double threshold;
    private File matched;
    private String mSeparator;
    private String mEncoding;
    private SimilarityComparator comparator;
    private int similarityLimitedLengthScope; // 相似度比对受限长度范围（指的是在正负 N 个长度范围内提取比对数据的范围）


    public SimilarityMemoryLimitedScopeThread(File f, String fSeparator, String fEncoding, File s, String sSeparator, String sEncoding, Double threshold, File matched, String mSeparator, String mEncoding, SimilarityComparator comparator, int similarityLimitedLengthScope) {

        this.f = f;
        this.fSeparator = fSeparator != null ? fSeparator : DEFAULT_SEPARATOR;
        this.fEncoding = fEncoding != null ? fEncoding : DEFAULT_ENCODING;
        this.s = s;
        this.sSeparator = sSeparator != null ? sSeparator : DEFAULT_SEPARATOR;

        this.sEncoding = sEncoding != null ? sEncoding : DEFAULT_ENCODING;
        this.threshold = threshold != null ? threshold : new Double("1.0");
        this.matched = matched;
        this.mSeparator = mSeparator != null ? mSeparator : DEFAULT_SEPARATOR;
        this.mEncoding = mEncoding != null ? mEncoding : DEFAULT_ENCODING;
        this.comparator = comparator != null ? comparator : new JaroWinklerDistanceComparator(this.threshold);

        this.similarityLimitedLengthScope = similarityLimitedLengthScope;
    }

    @Override
    public void run() {
        String threadName = Thread.currentThread().getName();
        long startTime = System.currentTimeMillis();

        List<String> ml = new ArrayList<String>();

        logger.info("@@@ 线程 {} 启动...", threadName);

        try {

            SortedCompareUnits fScus = new SortedCompareUnits(f, fEncoding, fSeparator);
            SortedCompareUnits sScus = new SortedCompareUnits(s, sEncoding, sSeparator);

            int currentLength = 0;
            CompareUnit[] currentLengthCompareUnits = null;
            int matchedCount = 0;
            int curLen = 0;
            for (CompareUnit fcu : fScus.getCompareUnits()) {
                curLen = fcu.length;
                if (currentLength > 0) {
                    if (currentLength < curLen) {
                        currentLength = curLen;
                        currentLengthCompareUnits = sScus.getLimitedCompareUnits(currentLength, similarityLimitedLengthScope);
                    }
                } else {
                    currentLength = curLen;
                    currentLengthCompareUnits = sScus.getLimitedCompareUnits(currentLength, similarityLimitedLengthScope);
                }

                logger.debug("@@@ 线程：{} \n 待比对数据长度：{} 受限范围：{} 提取比对数据长度：{} 提取比对数据记录数：{}", threadName, curLen, similarityLimitedLengthScope, currentLength, currentLengthCompareUnits.length);

                for (CompareUnit scu : currentLengthCompareUnits) {
                    // 比对内容长度小于 5 的进行精确匹配，大于等于 5 的进行相似度匹配
                    if (currentLength < 5) {
                        if (fcu.content.equals(scu.content)) {
                            ml.add(fcu.key + mSeparator + scu.key);
                            matchedCount++;
                            logger.debug("@@@ 线程：{} 已匹配 - {}\n匹配关键字\t{} - {}\n内容 1：{}\n内容 2：{}", threadName, matchedCount, fcu.key, scu.key, fcu.content, scu.content);
                        }
                    } else {
                        if (comparator.compare(fcu.content, scu.content)) {
                            ml.add(fcu.key + mSeparator + scu.key);
                            matchedCount++;
                            logger.debug("@@@ 线程：{} 已匹配 - {}\n匹配关键字\t{} - {}\n内容 1：{}\n内容 2：{}", threadName, matchedCount, fcu.key, scu.key, fcu.content, scu.content);
                        }
                    }
                }
            }

            FileUtils.writeLines(matched, mEncoding, ml);

            long endTime = System.currentTimeMillis();
            long consuming = (endTime - startTime) / 1000;
            logger.info("线程：{} 耗时 : {} ", threadName, (consuming / 60) > 0 ? (String.valueOf(consuming / 60) + " 分钟") : "小于 1 分钟 (约为 " + String.valueOf(consuming % 60) + " 秒)");

        } catch (IOException e) {
            logger.error("IO 异常", e);
        }
    }

}
