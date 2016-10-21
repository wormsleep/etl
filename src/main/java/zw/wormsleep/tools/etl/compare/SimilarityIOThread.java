package zw.wormsleep.tools.etl.compare;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * Created by wormsleep on 2015/12/29.
 */
public class SimilarityIOThread extends Thread {
    final Logger logger = LoggerFactory.getLogger(SimilarityIOThread.class);

    private final int BUFFER_SIZE = 100 * 1024;
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


    public SimilarityIOThread(File f, String fSeparator, String fEncoding, File s, String sSeparator, String sEncoding, Double threshold, File matched, String mSeparator, String mEncoding, SimilarityComparator comparator) {
        this.f = f;
        this.fSeparator = fSeparator != null ? fSeparator : DEFAULT_SEPARATOR;
        this.fEncoding = fEncoding != null ? fEncoding : DEFAULT_ENCODING;
        this.s = s;
        this.sSeparator = sSeparator != null ? sSeparator : DEFAULT_SEPARATOR;
        ;
        this.sEncoding = sEncoding != null ? sEncoding : DEFAULT_ENCODING;
        this.threshold = threshold != null ? threshold : new Double("1.0");
        this.matched = matched;
        this.mSeparator = mSeparator != null ? mSeparator : DEFAULT_SEPARATOR;
        this.mEncoding = mEncoding != null ? mEncoding : DEFAULT_ENCODING;
        this.comparator = comparator != null ? comparator : new JaroWinklerDistanceComparator(this.threshold);
    }

    @Override
    public void run() {
        String threadName = Thread.currentThread().getName();
        long startTime = System.currentTimeMillis();

        BufferedReader fReader = null;
        BufferedReader sReader = null;
        BufferedWriter mWriter = null;
        try {
            fReader = new BufferedReader(new InputStreamReader(new FileInputStream(f), fEncoding), BUFFER_SIZE);
            mWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(matched), mEncoding), BUFFER_SIZE);

            String fLine, sLine, first, second, fKey, sKey;
            int matchedCount = 0;
            int fIndex = 0;
            while ((fLine = fReader.readLine()) != null) {
                fIndex++;
                String[] fls = fLine.split(fSeparator);
                if (fls.length > 1) {
                    logger.debug("@@@ 线程：{} 待匹配记录：行 {} - {}", threadName, fIndex, fLine);
                    sReader = new BufferedReader(new InputStreamReader(new FileInputStream(s), sEncoding), BUFFER_SIZE);
                    while ((sLine = sReader.readLine()) != null) {
                        String[] sls = sLine.split(sSeparator);
                        if (sls.length > 1) {
                            first = fls[1];
                            second = sls[1];
                            if (comparator.compare(first, second)) {
                                fKey = fls[0];
                                sKey = sls[0];
                                logger.info("@@@ 线程：{} 匹配 - {}\n匹配关键字\t{} - {}\n内容 1：{}\n内容 2：{}", threadName, matchedCount, fKey, sKey, first, second);
                                if (matchedCount++ > 0) {
                                    mWriter.newLine();
                                }
                                mWriter.write(fKey + mSeparator + sKey);
                            }
                        }
                    }
                }
            }

            long endTime = System.currentTimeMillis();
            long consuming = (endTime - startTime) / 1000;
            logger.info("线程：{} 耗时 : {} ", threadName, (consuming / 60) > 0 ? (String.valueOf(consuming / 60) + " 分钟 " + (consuming % 60) + " 秒") : "小于 1 分钟");

        } catch (IOException e) {
            logger.error("IO 异常", e);
        } finally {
            if (fReader != null) {
                try {
                    fReader.close();
                } catch (IOException e) {
                    logger.error("IO 异常", e);
                }
                fReader = null;
            }

            if (sReader != null) {
                try {
                    sReader.close();
                } catch (IOException e) {
                    logger.error("IO 异常", e);
                }
                sReader = null;
            }

            if (mWriter != null) {
                try {
                    mWriter.flush();
                    mWriter.close();
                } catch (IOException e) {
                    logger.error("IO 异常", e);
                }
                mWriter = null;
            }
        }
    }
}
