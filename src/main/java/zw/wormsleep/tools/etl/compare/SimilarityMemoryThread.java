package zw.wormsleep.tools.etl.compare;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wormsleep on 2015/12/29.
 */
public class SimilarityMemoryThread extends Thread {
    final Logger logger = LoggerFactory.getLogger(SimilarityMemoryThread.class);

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


    public SimilarityMemoryThread(File f, String fSeparator, String fEncoding, File s, String sSeparator, String sEncoding, Double threshold, File matched, String mSeparator, String mEncoding, SimilarityComparator comparator) {
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
    }

    @Override
    public void run() {
        long startTime = System.currentTimeMillis();
        String threadName = Thread.currentThread().getName();

        List<String> ml = new ArrayList<String>();

        try {
            List<String> fl = FileUtils.readLines(f, fEncoding);
            List<String> sl = FileUtils.readLines(s, sEncoding);

            String first, second, fKey, sKey;
            int matchedCount = 0;
            int fIndex = 0;
            for (String fLine : fl) {
                fIndex++;
                String[] fls = fLine.split(fSeparator);
                if (fls.length > 1) {
//                    logger.debug("@@@ 线程：{} 待匹配记录：行 {} - {}", threadName, fIndex, fLine);
                    for (String sLine : sl) {
                        String[] sls = sLine.split(sSeparator);
                        if (sls.length > 1) {
                            first = fls[1];
                            second = sls[1];
                            if (comparator.compare(first, second)) {
                                matchedCount++;
                                fKey = fls[0];
                                sKey = sls[0];
                                ml.add(fKey + mSeparator + sKey);
                                logger.debug("@@@ 线程：{} 匹配 - {} (行号：{})\n匹配关键字\t{} - {}\n内容 1：{}\n内容 2：{}", threadName, matchedCount, fIndex, fKey, sKey, first, second);
                            }
                        }
                    }
                }
            }

            FileUtils.writeLines(matched, mEncoding, ml);

            long endTime = System.currentTimeMillis();
            long consuming = (endTime - startTime) / 1000;
            logger.info("线程：{} 耗时 : {} ", threadName, (consuming / 60) > 0 ? (String.valueOf(consuming / 60) + " 分钟 " + (consuming % 60) + " 秒") : "小于 1 分钟");

        } catch (IOException e) {
            logger.error("IO 异常", e);
        }
    }
}
