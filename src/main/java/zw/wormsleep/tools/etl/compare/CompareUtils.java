package zw.wormsleep.tools.etl.compare;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zw.wormsleep.tools.etl.utils.ThreadUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.Collator;
import java.util.*;
import java.util.concurrent.ExecutorService;

/**
 * Created by wormsleep on 2015/11/25.
 */
public class CompareUtils {
    final static Logger logger = LoggerFactory.getLogger(CompareUtils.class);

    private static final int BUFFER_SIZE = 10 * 1024 * 1024;
    private static final int COMPARE_SPLIT_LINE_SIZE = 5 * 1000;
    private static final int SORT_SPLIT_LINE_SIZE = 100 * 1000;
    private final static String SEPARATOR = "!@#";
    private final static String ENCODING = "UTF-8";
    private final static int LIMITED_LENGTH_SCOPE = 4;
    private final static int POWER = 2; // 多线程倍率
    private final static String LINE_SEPARATOR = (String) java.security.AccessController.doPrivileged(
            new sun.security.action.GetPropertyAction("line.separator"));

    /**
     * 相似度比较。
     * 通过逐行比较首文件和次文件内容并输出至已匹配文件。
     * 首文件和次文件每行内容格式为“关键字+分隔符+比对内容”
     * 输出文件每行内容格式为“首文件关键字+分隔符+次文件关键字”
     * <p/>
     * 默认值：分隔符 - TAB；文件编码 - UTF-8；比较器 - JarWinklerDistanceComparator；
     *
     * @param f         首文件
     * @param s         次文件
     * @param threshold 下限
     * @param matched   匹配输出文件
     */
    public static void similarity(File f, File s, Double threshold, File matched) {
        similarity(f, SEPARATOR, ENCODING, s, SEPARATOR, ENCODING, threshold, matched, SEPARATOR, ENCODING, new JaroWinklerDistanceComparator(threshold), COMPARE_SPLIT_LINE_SIZE, COMPARE_SPLIT_LINE_SIZE, LIMITED_LENGTH_SCOPE);
    }

    /**
     * 相似度比较。
     * 通过逐行比较首文件和次文件内容并输出至已匹配文件。
     * 首文件和次文件每行内容格式为“关键字+分隔符+比对内容”
     * 输出文件每行内容格式为“首文件关键字+分隔符+次文件关键字”
     * <p/>
     * 默认值：分隔符 - TAB；文件编码 - UTF-8；；
     *
     * @param f          首文件
     * @param s          次文件
     * @param threshold  下限
     * @param matched    匹配输出文件
     * @param comparator 比较器
     */
    public static void similarity(File f, File s, Double threshold, File matched, SimilarityComparator comparator) {
        similarity(f, SEPARATOR, ENCODING, s, SEPARATOR, ENCODING, threshold, matched, SEPARATOR, ENCODING, comparator, COMPARE_SPLIT_LINE_SIZE, COMPARE_SPLIT_LINE_SIZE, LIMITED_LENGTH_SCOPE);
    }

    /**
     * 相似度比较。
     * 通过逐行比较首文件和次文件内容并输出至已匹配文件。
     * 首文件和次文件每行内容格式为“关键字+分隔符+比对内容”
     * 输出文件每行内容格式为“首文件关键字+分隔符+次文件关键字”
     * <p/>
     * 默认值：比较器 - JarWinklerDistanceComparator；
     *
     * @param f         首文件
     * @param s         次文件
     * @param threshold 下限
     * @param matched   匹配输出文件
     * @param separator 分隔符（首文件、次文件、输出文件一致）
     * @param encoding  文件编码（首文件、次文件、输出文件一致）
     */
    public static void similarity(File f, File s, Double threshold, File matched, String separator, String encoding, SimilarityComparator comparator) {
        similarity(f, separator, encoding, s, separator, encoding, threshold, matched, separator, encoding, comparator, COMPARE_SPLIT_LINE_SIZE, COMPARE_SPLIT_LINE_SIZE, LIMITED_LENGTH_SCOPE);
    }

    /**
     * 相似度比较 - 不分割文件（单线程）。
     * 通过逐行比较首文件和次文件内容并输出至已匹配文件。
     * 首文件和次文件每行内容格式为“关键字+分隔符+比对内容”
     * 输出文件每行内容格式为“首文件关键字+分隔符+次文件关键字”
     *
     * @param f          首文件
     * @param fSeparator 首文件分隔符
     * @param fEncoding  首文件编码
     * @param s          次文件
     * @param sSeparator 次文件分隔符
     * @param sEncoding  次文件编码
     * @param threshold  下限
     * @param matched    匹配输出文件
     * @param mSeparator 匹配输出文件分隔符
     * @param mEncoding  匹配输出文件编码
     * @param comparator 比较器
     */
    protected static void similarity(File f, String fSeparator, String fEncoding, File s, String sSeparator, String sEncoding, Double threshold, File matched, String mSeparator, String mEncoding, SimilarityComparator comparator) {
        if (fSeparator == null) {
            fSeparator = SEPARATOR;
        }
        if (fEncoding == null) {
            fEncoding = ENCODING;
        }
        if (sSeparator == null) {
            sSeparator = SEPARATOR;
        }
        if (sEncoding == null) {
            sEncoding = ENCODING;
        }
        if (mSeparator == null) {
            mSeparator = SEPARATOR;
        }
        if (mEncoding == null) {
            mEncoding = ENCODING;
        }
        if (comparator == null) {
            comparator = new JaroWinklerDistanceComparator(threshold);
        }


        // 日志
        logger.info(
                "@@@ 相似度比较开始...\n" +
                        "**********\n" +
                        "比对环境信息\n" +
                        "**********\n" +
                        "首文件 \n" +
                        "\t路径: {} \n" +
                        "\t分隔符: {} \t 编码: {} \n" +
                        "次文件 \n" +
                        "\t路径: {} \n" +
                        "\t分隔符: {} \t 编码: {} \n" +
                        "输出文件 \n" +
                        "\t路径: {} \n" +
                        "\t分隔符: {} \t 编码: {} \n" +
                        "相似度评分基准值: {} \n",
                f.getAbsolutePath(),
                fSeparator.equals("\t") ? "TAB" : fSeparator,
                fEncoding,
                s.getAbsolutePath(),
                sSeparator.equals("\t") ? "TAB" : sSeparator,
                sEncoding,
                matched.getAbsolutePath(),
                mSeparator.equals("\t") ? "TAB" : mSeparator,
                mEncoding,
                threshold
        );

        long startTime = System.currentTimeMillis();

        // 比对主体
        BufferedReader fReader = null, sReader = null;
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
                    logger.debug("@@@ 待匹配记录：行 {} - {}", fIndex, fLine);
                    sReader = new BufferedReader(new InputStreamReader(new FileInputStream(s), sEncoding), BUFFER_SIZE);
                    while ((sLine = sReader.readLine()) != null) {
                        String[] sls = sLine.split(sSeparator);
                        if (sls.length > 1) {
                            first = fls[1];
                            second = sls[1];
                            if (comparator.compare(first, second)) {
                                fKey = fls[0];
                                sKey = sls[0];
                                logger.debug("@@@ 匹配 - {}\n匹配关键字\t{} - {}\n内容 1：{}\n内容 2：{}", matchedCount, fKey, sKey, first, second);
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
            logger.info("耗时 : {} ", (consuming / 60) > 0 ? (String.valueOf(consuming / 60) + " 分钟") : "小于 1 分钟");

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

    /**
     * 相似度比较 - 首文件和次文件分割（多线程）。
     * 通过逐行比较首文件和次文件内容并输出至已匹配文件。
     * 首文件和次文件每行内容格式为“关键字+分隔符+比对内容”
     * 输出文件每行内容格式为“首文件关键字+分隔符+次文件关键字”
     *
     * @param f              首文件
     * @param fSeparator     首文件分隔符
     * @param fEncoding      首文件编码
     * @param s              次文件
     * @param sSeparator     次文件分隔符
     * @param sEncoding      次文件编码
     * @param threshold      下限
     * @param matched        匹配输出文件
     * @param mSeparator     匹配输出文件分隔符
     * @param mEncoding      匹配输出文件编码
     * @param comparator     比较器
     * @param fSplitLineSize 首文件 分割行数/每文件
     * @param sSplitLineSize 次文件 分割行数/每文件
     */
    public static void similarity(File f, String fSeparator, String fEncoding, File s, String sSeparator, String sEncoding, Double threshold, File matched, String mSeparator, String mEncoding, SimilarityComparator comparator, int fSplitLineSize, int sSplitLineSize) {
        // 日志
        logger.info(
                "@@@ 相似度比较开始...\n" +
                        "***********\n" +
                        "比对环境信息\n" +
                        "***********\n" +
                        "首文件 \n" +
                        "\t路径: {} \n" +
                        "\t分隔符: {} \t 编码: {} \n" +
                        "\t分割行数：{}\n" +
                        "次文件 \n" +
                        "\t路径: {} \n" +
                        "\t分隔符: {} \t 编码: {} \n" +
                        "\t分割行数：{}\n" +
                        "输出文件 \n" +
                        "\t路径: {} \n" +
                        "\t分隔符: {} \t 编码: {} \n" +
                        "相似度评分基准值: {} \n" +
                        "相似度比较器：{} \n",
                f.getAbsolutePath(),
                fSeparator.equals("\t") ? "TAB" : fSeparator,
                fEncoding,
                fSplitLineSize,
                s.getAbsolutePath(),
                sSeparator.equals("\t") ? "TAB" : sSeparator,
                sEncoding,
                fSplitLineSize,
                matched.getAbsolutePath(),
                mSeparator.equals("\t") ? "TAB" : mSeparator,
                mEncoding,
                threshold,
                comparator.getClass().getSimpleName()
        );

        long startTime = System.currentTimeMillis();

        List<File> fParts = splitFile(f, fEncoding, fSplitLineSize);
        List<File> sParts = splitFile(s, sEncoding, sSplitLineSize);
        List<File> mParts = new ArrayList<File>();
        int fPartsCount = fParts.size();
        int sPartsCount = sParts.size();
        File fPart = null;
        File sPart = null;
        File mPart = null;
        String path = matched.getAbsolutePath();
        String mFullPath = FilenameUtils.getFullPath(path);
        String mFilename = FilenameUtils.getBaseName(path);

        ExecutorService pool = ThreadUtils.smartSimilarityThreadPool(fPartsCount, sPartsCount, POWER);

        for (int fi = 0; fi < fPartsCount; fi++) {

            fPart = fParts.get(fi);

            for (int si = 0; si < sPartsCount; si++) {

                sPart = sParts.get(si);

                mPart = new File(mFullPath + mFilename + "-" + String.valueOf(fi + 1) + "-" + String.valueOf(si + 1));
                mParts.add(mPart);

                pool.execute(new SimilarityMemoryThread(fPart, fSeparator, fEncoding, sPart, sSeparator, sEncoding, threshold, mPart, mSeparator, mEncoding, comparator));

            }
        }

        pool.shutdown();

        while (true) {
            if (pool.isTerminated()) {
                logger.info("@@@ 相似度匹配（多线程）完成... 准备合并匹配结果文件。");
                break;
            }
        }

        mergeFiles(mParts, matched);

        long endTime = System.currentTimeMillis();
        long consuming = (endTime - startTime) / 1000;
        logger.info("@@@ 相似度（多线程）比较耗时 : {} ", (consuming / 60) > 0 ? (String.valueOf(consuming / 60) + " 分钟") : "小于 1 分钟 (约为 " + String.valueOf(consuming % 60) + " 秒)");

    }

    /**
     * 相似度比较 - 首文件和次文件分割（多线程）。
     * 通过逐行比较首文件和次文件内容并输出至已匹配文件。
     * 首文件和次文件每行内容格式为“关键字+分隔符+比对内容”
     * 输出文件每行内容格式为“首文件关键字+分隔符+次文件关键字”
     *
     * @param f                            首文件
     * @param fSeparator                   首文件分隔符
     * @param fEncoding                    首文件编码
     * @param s                            次文件
     * @param sSeparator                   次文件分隔符
     * @param sEncoding                    次文件编码
     * @param threshold                    下限
     * @param matched                      匹配输出文件
     * @param mSeparator                   匹配输出文件分隔符
     * @param mEncoding                    匹配输出文件编码
     * @param comparator                   比较器
     * @param fSplitLineSize               首文件 分割行数/每文件
     * @param sSplitLineSize               次文件 分割行数/每文件
     * @param similarityLimitedLengthScope 相似度比对受限长度范围
     */
    public static void similarity(File f, String fSeparator, String fEncoding, File s,
                                  String sSeparator, String sEncoding,
                                  Double threshold, File matched, String mSeparator, String mEncoding,
                                  SimilarityComparator comparator, int fSplitLineSize, int sSplitLineSize,
                                  int similarityLimitedLengthScope) {
        // 日志
        logger.info(
                "@@@ 相似度比较开始...\n" +
                        "***********\n" +
                        "比对环境信息\n" +
                        "***********\n" +
                        "首文件 \n" +
                        "\t路径: {} \n" +
                        "\t分隔符: {} \t 编码: {} \n" +
                        "\t分割行数：{}\n" +
                        "次文件 \n" +
                        "\t路径: {} \n" +
                        "\t分隔符: {} \t 编码: {} \n" +
                        "\t分割行数：{}\n" +
                        "输出文件 \n" +
                        "\t路径: {} \n" +
                        "\t分隔符: {} \t 编码: {} \n" +
                        "相似度评分基准值: {} \n" +
                        "相似度比较器：{} \n",
                f.getAbsolutePath(),
                fSeparator.equals("\t") ? "TAB" : fSeparator,
                fEncoding,
                fSplitLineSize,
                s.getAbsolutePath(),
                sSeparator.equals("\t") ? "TAB" : sSeparator,
                sEncoding,
                fSplitLineSize,
                matched.getAbsolutePath(),
                mSeparator.equals("\t") ? "TAB" : mSeparator,
                mEncoding,
                threshold,
                comparator.getClass().getSimpleName()
        );

        long startTime = System.currentTimeMillis();

        List<File> fParts = splitFile(f, fEncoding, fSplitLineSize);
        List<File> sParts = splitFile(s, sEncoding, sSplitLineSize);
        List<File> mParts = new ArrayList<File>();
        int fPartsCount = fParts.size();
        int sPartsCount = sParts.size();
        File fPart = null;
        File sPart = null;
        File mPart = null;
        String path = matched.getAbsolutePath();
        String mFullPath = FilenameUtils.getFullPath(path);
        String mFilename = FilenameUtils.getBaseName(path);

        ExecutorService pool = ThreadUtils.smartSimilarityThreadPool(fPartsCount, sPartsCount, POWER);

        for (int fi = 0; fi < fPartsCount; fi++) {

            fPart = fParts.get(fi);

            for (int si = 0; si < sPartsCount; si++) {

                sPart = sParts.get(si);

                mPart = new File(mFullPath + mFilename + "-" + String.valueOf(fi + 1) + "-" + String.valueOf(si + 1));
                mParts.add(mPart);

                pool.execute(new SimilarityMemoryLimitedScopeThread(fPart, fSeparator, fEncoding, sPart, sSeparator, sEncoding, threshold, mPart, mSeparator, mEncoding, comparator, similarityLimitedLengthScope));

            }
        }

        pool.shutdown();

        while (true) {
            if (pool.isTerminated()) {
                logger.info("@@@ 相似度匹配（多线程）完成... 准备合并匹配结果文件。");
                break;
            }
            // 每隔一段时间判断线程是否完成
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        mergeFiles(mParts, matched);

        long endTime = System.currentTimeMillis();
        long consuming = (endTime - startTime) / 1000;
        logger.info("@@@ 相似度（多线程）比较耗时 : {} ", (consuming / 60) > 0 ? (String.valueOf(consuming / 60) + " 分钟") : "小于 1 分钟 (约为 " + String.valueOf(consuming % 60) + " 秒)");

    }

    /**
     * 将文件按指定行数分割为多个文件。
     *
     * @param file     文件
     * @param encoding 文件编码
     * @param lineSize 每个文件的最大行数
     * @return 分割的文件列表
     */
    public static List<File> splitFile(File file, String encoding, int lineSize) {
        List<File> files = new ArrayList<File>();

        String path = file.getAbsolutePath();
        String fullPath = FilenameUtils.getFullPath(path);
        String filename = FilenameUtils.getBaseName(path);
        String extension = FilenameUtils.getExtension(path);

        BufferedReader reader = null;
        BufferedWriter writer = null;

        File part = null;

        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), encoding), BUFFER_SIZE);

            int lineCount = 0;
            int fileCount = 0;
            String line = null;

            part = new File(fullPath + filename + "-" + String.valueOf(++fileCount));
            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(part), encoding), BUFFER_SIZE);
            files.add(part);

            int lineTotalCount = 0;
            while ((line = reader.readLine()) != null) {
                if (lineCount++ < lineSize) {
                    writer.write(line + LINE_SEPARATOR);
                } else {
                    writer.flush();

                    part = new File(fullPath + filename + "-" + String.valueOf(++fileCount));
                    writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(part), encoding), BUFFER_SIZE);
                    files.add(part);

                    writer.write(line + LINE_SEPARATOR);
                    lineCount = 1;
                }
                lineTotalCount++;
            }

            logger.info("@@@ 文件分割 ... \n原文件：{}, 共 {} 行；\n按每个文件 {} 行进行分割，共计 {} 个子文件，末文件行数 {}；",
                    filename,
                    lineTotalCount,
                    lineSize,
                    files.size(),
                    lineCount);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                    reader = null;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (writer != null) {
                try {
                    writer.flush();
                    writer.close();
                    writer = null;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return files;
    }

    /**
     * 合并多个文件
     *
     * @param files
     * @param out
     */
    public static void mergeFiles(List<File> files, File out) {
        FileChannel outChl = null;
        FileChannel inChl = null;
        try {
            outChl = new FileOutputStream(out).getChannel();
            ByteBuffer bb = null;
            int count = 0;
            for (File file : files) {
                inChl = new FileInputStream(file).getChannel();
                bb = ByteBuffer.allocate(BUFFER_SIZE);
                if (count > 0) {
                    ByteBuffer crlf = ByteBuffer.wrap("\r\n".getBytes());
                    outChl.write(crlf);
                }
                while (inChl.read(bb) != -1) {
                    bb.flip();
                    outChl.write(bb);
                    bb.clear();
                }
                inChl.close();
                count++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (outChl != null) {
                try {
                    outChl.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    public static void sortFile(File src, String separator, int fieldIndex) throws IOException {
        sortFile(src, ENCODING, separator, fieldIndex, true, null, SORT_SPLIT_LINE_SIZE);
    }

    public static void sortFile(File src, String separator, int fieldIndex, boolean allowEmptyField) throws IOException {
        sortFile(src, ENCODING, separator, fieldIndex, allowEmptyField, null, SORT_SPLIT_LINE_SIZE);
    }

    public static void sortFile(File src, String separator,
                                int fieldIndex, boolean allowEmptyField,
                                Comparator<KeyValue> comparator, int splitLineSize) throws IOException {
        sortFile(src, ENCODING, separator, fieldIndex, allowEmptyField, comparator, splitLineSize);
    }

    public static void sortFile(File src, String encoding, String separator, int fieldIndex) throws IOException {
        sortFile(src, encoding, separator, fieldIndex, true, null, SORT_SPLIT_LINE_SIZE);
    }

    public static void sortFile(File src, String encoding, String separator, int fieldIndex, boolean allowEmptyField) throws IOException {
        sortFile(src, encoding, separator, fieldIndex, allowEmptyField, null, SORT_SPLIT_LINE_SIZE);
    }

    /**
     * 对文件内容按指定域排序
     *
     * @param src             源文件
     * @param encoding        文件编码
     * @param separator       域分隔符
     * @param fieldIndex      指定排序字段位置（从 0 开始）
     * @param allowEmptyField 是否过滤排序字段值为空的记录
     * @param comparator      KeyValue 对象比较器
     * @param splitLineSize   子文件分割最大行数
     */
    public static void sortFile(File src, String encoding, String separator,
                                int fieldIndex, boolean allowEmptyField,
                                Comparator<KeyValue> comparator, int splitLineSize) throws IOException {

        logger.info("@@@ 文件域排序开始……\n" +
                        "***********\n" +
                        "文件域排序信息\n" +
                        "***********\n" +
                        "文件：{}\n" +
                        "编码：{}\n" +
                        "分隔符：{}\n" +
                        "排序域：{} (从 0 开始)\n" +
                        "允许空域：{}",
                src.getAbsolutePath(),
                encoding,
                separator,
                fieldIndex,
                allowEmptyField ? "是" : "否");

        long startTime = System.currentTimeMillis();

        // 比较器默认提供
        comparator = comparator != null ? comparator : new Comparator<KeyValue>() {
            @Override
            public int compare(KeyValue o1, KeyValue o2) {
                return Collator.getInstance(Locale.CHINA).compare(o1.getKey(), o2.getKey());
            }
        };

        // 分割文件
        List<File> sfs = splitFile(src, encoding, splitLineSize);
        // 排序文件 - 多线程排序分割文件

        ExecutorService pool = ThreadUtils.smartSortThreadPool(sfs.size(), 1);

        for (Iterator<File> it = sfs.iterator(); it.hasNext(); ) {
            pool.execute(new SortSmallFileThread(it.next(), encoding, separator, fieldIndex, allowEmptyField, comparator));
        }

        pool.shutdown();

        while (true) {
            if (pool.isTerminated()) {
                logger.info("@@@ 子文件排序（多线程）完成！ 准备归并排序……。");
                break;
            }
            // 每隔一段时间判断线程是否完成
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // Reader 列表
        List<BufferedReader> readers = new ArrayList<BufferedReader>();
        BufferedWriter writer = null;

        try {
            // readers 初始化
            int bufferSize = 10 * 1024; // 目前先固化 buffer size 大小以后将按内存动态计算
            for (Iterator<File> it = sfs.iterator(); it.hasNext(); ) {
                readers.add(new BufferedReader(new InputStreamReader(new FileInputStream(it.next()), encoding), bufferSize));
            }

            // 归并
            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(src), encoding), BUFFER_SIZE);

            int count = sfs.size();
            String[] lines = new String[count];
            Boolean[] done = new Boolean[count];
            for (int i = 0; i < done.length; done[i] = false, i++) ;
            int doneCount = 0;
            String line;

            int indexOfMinKeyReader = -1;

            // 准备首次比对数据
            for (int i = 0; i < count; i++) {
                line = readers.get(i).readLine();
                if (line != null) {
                    lines[i] = line;
                } else {
                    done[i] = true;
                    doneCount++;
                }
            }

            int lcount = 0;
            // 循环退出条件 - 所有的文件都已经处理完毕（已按行读取完毕）
            while (doneCount < count) {

                // 进行比对 - 按最小值算法
                indexOfMinKeyReader = getMinimumFieldIndex(lines, separator, fieldIndex, comparator);
                // 输出最小值行
                if (indexOfMinKeyReader < 0) {
                    break;
                } else {
                    writer.write(lines[indexOfMinKeyReader] + LINE_SEPARATOR);
//                    logger.debug("处理计数：{} 输出文件索引：{} 输出行: {}", lcount, indexOfMinKeyReader, lines[indexOfMinKeyReader]);
                    // 准备下一次比较数据 - 从最小值行对应的文件中提取下一行
                    line = done[indexOfMinKeyReader] ? null : readers.get(indexOfMinKeyReader).readLine();
                    lines[indexOfMinKeyReader] = line;
                    if (line == null) {
                        logger.info("@@@  子文件 {} 读取完毕！", indexOfMinKeyReader);
                        done[indexOfMinKeyReader] = true;
                        doneCount++;
                    }

                }

                lcount++;
            }

            long endTime = System.currentTimeMillis();
            long consuming = (endTime - startTime) / 1000;
            logger.info("@@@ 文件域排序结束！\n总计：{} 行 - 耗时： {} ", lcount, (consuming / 60) > 0 ? (String.valueOf(consuming / 60) + " 分钟") : "小于 1 分钟 (约为 " + String.valueOf(consuming % 60) + " 秒)");

        } finally {
            for (Iterator<BufferedReader> it = readers.iterator(); it.hasNext(); IOUtils.closeQuietly(it.next())) ;
            IOUtils.closeQuietly(writer);
        }
    }

    /**
     * 获取最小字段所在行索引
     *
     * @param lines
     * @param separator
     * @param fieldIndex
     * @return
     */
    private static int getMinimumFieldIndex(String[] lines, String separator, int fieldIndex, Comparator<KeyValue> comparator) {
        int minIndex = -1;
        int count = lines.length;
        KeyValue tmpKeyValue = null;
        KeyValue minKeyValue = null;
        String[] ls = null;
        for (int i = 0; i < count; i++) {
            if (lines[i] != null) {
                ls = lines[i].split(separator);
                tmpKeyValue = new KeyValue(ls[fieldIndex], lines[i]);
            } else {
                tmpKeyValue = null;
            }
            if (tmpKeyValue != null && minKeyValue != null) {
                if (comparator.compare(minKeyValue, tmpKeyValue) > 0) {
                    minKeyValue = tmpKeyValue;
                    minIndex = i;
                }
            } else {
                if (tmpKeyValue != null) {
                    minKeyValue = tmpKeyValue;
                    minIndex = i;
                }
            }
        }

        return minIndex;
    }

}
