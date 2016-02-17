package zw.wormsleep.tools.etl.compare;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.Collator;
import java.util.*;

/**
 * 小文件排序线程
 *
 * Created by wormsleep on 2016/2/14.
 */
public class SortSmallFileThread extends Thread {

    final Logger logger = LoggerFactory.getLogger(SortSmallFileThread.class);

    private File inout;
    private String encoding;
    private String separator;
    private int fieldIndex;
    private boolean allowEmptyField;

    private List<String> lines;
    private Comparator<KeyValue> comparator;

    public SortSmallFileThread(File inout, String encoding, String separator, int fieldIndex, boolean allowEmptyField, Comparator<KeyValue> comparator) throws IOException {
        this.inout = inout;
        lines = FileUtils.readLines(inout, encoding);
        this.encoding = encoding != null ? encoding : "UTF-8";
        this.separator = separator != null ? separator : "\t";
        this.fieldIndex = fieldIndex;
        this.allowEmptyField = allowEmptyField;
        this.comparator = comparator;
    }

    @Override
    public void run() {
        long startTime = System.currentTimeMillis();
        String threadName = Thread.currentThread().getName();
        logger.info("@@@ 线程：{} - 编码：{} - 分隔符：{} - 过滤空字段：{} - 开始……", threadName, encoding, separator, allowEmptyField);

        List<KeyValue> keyValues = new ArrayList<KeyValue>();

        // 过滤并生成有效数据集
        String[] ls;
        String key = null;
        for (String line : lines) {
            ls = line.split(separator);
            if (ls.length > fieldIndex) {
                key = ls[fieldIndex];
                if (allowEmptyField) {
                    keyValues.add(new KeyValue(key, line));
                } else {
                    if (!key.trim().equals("")) {
                        keyValues.add(new KeyValue(key, line));
                    }
                }
            }
        }

        // 排序
        KeyValue[] kvs = keyValues.toArray(new KeyValue[keyValues.size()]);
        Arrays.sort(kvs, comparator);
        keyValues = null;

        keyValues = new ArrayList<KeyValue>();
        for (KeyValue kv : kvs) {
            keyValues.add(kv);
        }

        try {
            FileUtils.writeLines(inout, encoding, keyValues);
            long endTime = System.currentTimeMillis();
            long consuming = (endTime - startTime) / 1000;
            logger.info("@@@ 线程：{} - 结束！耗时：{}", threadName, (consuming / 60) > 0 ? (String.valueOf(consuming / 60) + " 分钟") : "小于 1 分钟 (约为 " + String.valueOf(consuming % 60) + " 秒)");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
