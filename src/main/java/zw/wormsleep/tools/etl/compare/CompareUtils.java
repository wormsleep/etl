package zw.wormsleep.tools.etl.compare;

import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by wormsleep on 2015/11/25.
 */
public class CompareUtils {
    final static Logger logger = LoggerFactory.getLogger(CompareUtils.class);

    private static final int BUFFER_SIZE = 10 * 1024 * 1024;
    private static final int SPLIT_LINE_SIZE = 5000;
    private final static String SEPARATOR = "!@#";
    private final static String ENCODING = "UTF-8";
    private final static int LIMITED_LENGTH_SCOPE = 4;

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
        similarity(f, SEPARATOR, ENCODING, s, SEPARATOR, ENCODING, threshold, matched, SEPARATOR, ENCODING, new JaroWinklerDistanceComparator(threshold), SPLIT_LINE_SIZE, SPLIT_LINE_SIZE, LIMITED_LENGTH_SCOPE);
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
        similarity(f, SEPARATOR, ENCODING, s, SEPARATOR, ENCODING, threshold, matched, SEPARATOR, ENCODING, comparator, SPLIT_LINE_SIZE, SPLIT_LINE_SIZE, LIMITED_LENGTH_SCOPE);
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
        similarity(f, separator, encoding, s, separator, encoding, threshold, matched, separator, encoding, comparator, SPLIT_LINE_SIZE, SPLIT_LINE_SIZE, LIMITED_LENGTH_SCOPE);
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
            fSeparator = "\t";
        }
        if (fEncoding == null) {
            fEncoding = "UTF-8";
        }
        if (sSeparator == null) {
            sSeparator = "\t";
        }
        if (sEncoding == null) {
            sEncoding = "UTF-8";
        }
        if (mSeparator == null) {
            mSeparator = "\t";
        }
        if (mEncoding == null) {
            mEncoding = "UTF-8";
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

        ExecutorService pool = smartThreadPool(fPartsCount, sPartsCount);

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
        logger.info("@@@ 相似度（多线程）比较耗时 : {} ", (consuming / 60) > 0 ? (String.valueOf(consuming / 60) + " 分钟") : "小于 1 分钟 (约为 "+String.valueOf(consuming % 60)+" 秒)");

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

        ExecutorService pool = smartThreadPool(fPartsCount, sPartsCount);

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
        logger.info("@@@ 相似度（多线程）比较耗时 : {} ", (consuming / 60) > 0 ? (String.valueOf(consuming / 60) + " 分钟") : "小于 1 分钟 (约为 "+String.valueOf(consuming % 60)+" 秒)");

    }

    /**
     * 将文件按指定行数分割为多个文件。
     *
     * @param file     文件
     * @param encoding 文件编码
     * @param lines    每个文件的最大行数
     * @return 分割的文件列表
     */
    public static List<File> splitFile(File file, String encoding, int lines) {
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
                if (lineCount < lines) {
                    if (lineCount++ > 0) {
                        writer.newLine();
                    }
                    writer.write(line);
                } else {
                    writer.flush();

                    part = new File(fullPath + filename + "-" + String.valueOf(++fileCount));
                    writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(part), encoding), BUFFER_SIZE);
                    files.add(part);

                    writer.write(line);
                    lineCount = 1;
                }
                lineTotalCount++;
            }

            logger.info("@@@ 文件分割 ... \n原文件：{}, 共 {} 行；\n按每个文件 {} 行进行分割，共计 {} 个子文件，末文件行数 {}；",
                    filename,
                    lineTotalCount,
                    lines,
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

    /**
     * 文件内容记录重复清除
     *
     * @param src  原文件
     * @param dest 新文件
     */
    public static void removeDuplicateFileContent(File src, File dest, String encoding) {
        BufferedReader reader = null;
        BufferedWriter writer = null;

        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(src), encoding), BUFFER_SIZE);
            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(dest), encoding), BUFFER_SIZE);

            Set<String> contents = new HashSet<String>();
            String line = null;
            while ((line = reader.readLine()) != null) {
                contents.add(line); // 去重复
            }

            int count = 0;
            for (String c : contents) {
                if (count++ > 0) {
                    writer.newLine();
                }
                writer.write(c);
            }
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
    }

    /**
     * 交换文件域内容（仅针对有且仅有两个域内容的文件）
     *
     * @param src       原文件
     * @param dest      新文件
     * @param encoding
     * @param separator
     */
    public static void reverseTwoDomainFileContent(File src, File dest, String encoding, String separator) {
        BufferedReader reader = null;
        BufferedWriter writer = null;

        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(src), encoding), BUFFER_SIZE);
            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(dest), encoding), BUFFER_SIZE);

            String line = null;
            int count = 0;
            while ((line = reader.readLine()) != null) {
                String[] ll = line.split(separator);
                if (ll.length > 1) {
                    if (count++ > 0) {
                        writer.newLine();
                    }
                    writer.write(ll[1] + separator + ll[0]);
                }
            }

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
    }

    /**
     * 排序小文件内容
     *
     * @param src
     * @param dest
     * @param encoding
     * @param separator
     */
    public static void sortSamllFileContent(File src, File dest, String encoding, String separator) {
        BufferedReader reader = null;
        BufferedWriter writer = null;

        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(src), encoding), BUFFER_SIZE);
            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(dest), encoding), BUFFER_SIZE);

            String line = null;
            SortedSet<String> ss = new TreeSet<String>(new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                    return o1.compareTo(o2);
                }
            });

            while ((line = reader.readLine()) != null) {
                ss.add(line);
            }

            Iterator<String> iter = ss.iterator();
            String c = null;
            int count = 0;
            while (iter.hasNext()) {
                c = iter.next();
                if (count++ > 0) {
                    writer.newLine();
                }
                writer.write(c);
            }

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
    }

    // 根据操作系统 CPU 数量和比对文件数量乘积判定生成的线程池（类型）
    private static ExecutorService smartThreadPool(int fSize, int sSize) {
        return (Runtime.getRuntime().availableProcessors() * 10 > fSize * sSize) ? Executors.newCachedThreadPool() : Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 10);
    }

}
