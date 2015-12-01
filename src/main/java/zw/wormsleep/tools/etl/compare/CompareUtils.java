package zw.wormsleep.tools.etl.compare;

import org.apache.commons.collections.Bag;
import org.apache.commons.collections.bag.HashBag;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zw.wormsleep.tools.etl.utils.Uuid;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.Key;
import java.util.*;

/**
 * Created by wormsleep on 2015/11/25.
 */
public class CompareUtils {
    private static Logger logger = LoggerFactory.getLogger(CompareUtils.class);

    private static final int BUFFER_SIZE = 20 * 1024 * 1024;

    /**
     * 相似度比较。
     * 通过逐行比较首文件和次文件内容并输出至已匹配文件。
     * 首文件和次文件每行内容格式为“关键字+分隔符+比对内容”
     * 输出文件每行内容格式为“首文件关键字+分隔符+次文件关键字”
     *
     * 默认值：分隔符 - TAB；文件编码 - UTF-8；比较器 - JarWinklerDistanceComparator；
     *
     * @param f 首文件
     * @param s 次文件
     * @param threshold 下限
     * @param matched 匹配输出文件
     */
    public static void similarity(File f, File s, Double threshold, File matched) {
        similarity(f, null, null, s, null, null, threshold, matched, null, null, null);
    }

    /**
     * 相似度比较。
     * 通过逐行比较首文件和次文件内容并输出至已匹配文件。
     * 首文件和次文件每行内容格式为“关键字+分隔符+比对内容”
     * 输出文件每行内容格式为“首文件关键字+分隔符+次文件关键字”
     *
     * 默认值：分隔符 - TAB；文件编码 - UTF-8；；
     * @param f 首文件
     * @param s 次文件
     * @param threshold 下限
     * @param matched 匹配输出文件
     * @param comparator 比较器
     */
    public static void similarity(File f, File s, Double threshold, File matched, SimilarityComparator comparator) {
        similarity(f, null, null, s, null, null, threshold, matched, null, null, comparator);
    }

    /**
     * 相似度比较。
     * 通过逐行比较首文件和次文件内容并输出至已匹配文件。
     * 首文件和次文件每行内容格式为“关键字+分隔符+比对内容”
     * 输出文件每行内容格式为“首文件关键字+分隔符+次文件关键字”
     *
     * 默认值：文件编码 - UTF-8；比较器 - JarWinklerDistanceComparator；
     *
     * @param f 首文件
     * @param s 次文件
     * @param threshold 下限
     * @param matched 匹配输出文件
     * @param separator 分隔符（首文件、次文件、输出文件一致）
     */
    public static void similarity(File f, File s, Double threshold, File matched, String separator) {
        similarity(f, separator, null, s, separator, null, threshold, matched, separator, null, null);
    }

    /**
     * 相似度比较。
     * 通过逐行比较首文件和次文件内容并输出至已匹配文件。
     * 首文件和次文件每行内容格式为“关键字+分隔符+比对内容”
     * 输出文件每行内容格式为“首文件关键字+分隔符+次文件关键字”
     *
     * 默认值：文件编码 - UTF-8；比较器 - JarWinklerDistanceComparator；
     *
     * @param f 首文件
     * @param s 次文件
     * @param threshold 下限
     * @param matched 匹配输出文件
     * @param separator 分隔符（首文件、次文件、输出文件一致）
     * @param comparator 比较器
     */
    public static void similarity(File f, File s, Double threshold, File matched, String separator, SimilarityComparator comparator) {
        similarity(f, separator, null, s, separator, null, threshold, matched, separator, null, comparator);
    }

    /**
     * 相似度比较。
     * 通过逐行比较首文件和次文件内容并输出至已匹配文件。
     * 首文件和次文件每行内容格式为“关键字+分隔符+比对内容”
     * 输出文件每行内容格式为“首文件关键字+分隔符+次文件关键字”
     *
     * 默认值：比较器 - JarWinklerDistanceComparator；
     *
     * @param f 首文件
     * @param s 次文件
     * @param threshold 下限
     * @param matched 匹配输出文件
     * @param separator 分隔符（首文件、次文件、输出文件一致）
     * @param encoding 文件编码（首文件、次文件、输出文件一致）
     */
    public static void similarity(File f, File s, Double threshold, File matched, String separator, String encoding) {
        similarity(f, separator, encoding, s, separator, encoding, threshold, matched, separator, encoding, null);
    }

    /**
     * 相似度比较。
     * 通过逐行比较首文件和次文件内容并输出至已匹配文件。
     * 首文件和次文件每行内容格式为“关键字+分隔符+比对内容”
     * 输出文件每行内容格式为“首文件关键字+分隔符+次文件关键字”
     *
     * @param f 首文件
     * @param s 次文件
     * @param threshold 下限
     * @param matched 匹配输出文件
     * @param separator 分隔符（首文件、次文件、输出文件一致）
     * @param encoding 文件编码（首文件、次文件、输出文件一致）
     * @param comparator 比较器
     */
    public static void similarity(File f, File s, Double threshold, File matched, String separator, String encoding, SimilarityComparator comparator) {
        similarity(f, separator, encoding, s, separator, encoding, threshold, matched, separator, encoding, comparator);
    }

    /**
     * 相似度比较。
     * 通过逐行比较首文件和次文件内容并输出至已匹配文件。
     * 首文件和次文件每行内容格式为“关键字+分隔符+比对内容”
     * 输出文件每行内容格式为“首文件关键字+分隔符+次文件关键字”
     * @param f 首文件
     * @param fSeparator 首文件分隔符
     * @param fEncoding 首文件编码
     * @param s 次文件
     * @param sSeparator 次文件分隔符
     * @param sEncoding 次文件编码
     * @param threshold 下限
     * @param matched 匹配输出文件
     * @param mSeparator 匹配输出文件分隔符
     * @param mEncoding 匹配输出文件编码
     * @param comparator 比较器
     */
    public static void similarity(File f, String fSeparator, String fEncoding, File s, String sSeparator, String sEncoding, Double threshold, File matched, String mSeparator, String mEncoding, SimilarityComparator comparator) {
        if(fSeparator == null) {
            fSeparator = "\t";
        }
        if(fEncoding == null) {
            fEncoding = "UTF-8";
        }
        if(sSeparator == null) {
            sSeparator = "\t";
        }
        if(sEncoding == null) {
            sEncoding = "UTF-8";
        }
        if(mSeparator == null) {
            mSeparator = "\t";
        }
        if(mEncoding == null) {
            mEncoding = "UTF-8";
        }
        if(comparator == null) {
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
            while((fLine = fReader.readLine()) != null) {
                fIndex++;
                String[] fls = fLine.split(fSeparator);
                if(fls.length > 1) {
                    logger.debug("@@@ 待匹配记录：行 {} - {}", fIndex, fLine);
                    sReader = new BufferedReader(new InputStreamReader(new FileInputStream(s), sEncoding), BUFFER_SIZE);
                    while((sLine = sReader.readLine()) != null) {
                        String[] sls = sLine.split(sSeparator);
                        if(sls.length > 1) {
                            first = fls[1];
                            second = sls[1];
                            if(comparator.compare(first, second)) {
                                fKey = fls[0];
                                sKey = sls[0];
                                logger.info("@@@ 匹配 - {}\n匹配关键字\t{} - {}\n内容 1：{}\n内容 2：{}", matchedCount, fKey, sKey, first, second);
                                if(matchedCount++ > 0) {
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
            if(fReader != null) {
                try {
                    fReader.close();
                } catch (IOException e) {
                    logger.error("IO 异常", e);
                }
                fReader = null;
            }

            if(sReader != null) {
                try {
                    sReader.close();
                } catch (IOException e) {
                    logger.error("IO 异常", e);
                }
                sReader = null;
            }

            if(mWriter != null) {
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
     * 合并多个文件
     * @param files
     * @param out
     */
    public static void mergeFiles(List<File> files, File out) {
        FileChannel outChl = null;
        FileChannel inChl = null;
        try{
            outChl = new FileOutputStream(out).getChannel();
            ByteBuffer bb = null;
            int count = 0;
            for(File file : files) {
                inChl = new FileInputStream(file).getChannel();
                bb = ByteBuffer.allocate(BUFFER_SIZE);
                if(count > 0) {
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
        }catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(outChl != null) {
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
     * @param src 原文件
     * @param dest 新文件
     */
    public static void removeDuplicateFileContent(File src, File dest, String encoding) {
        BufferedReader reader = null;
        BufferedWriter writer = null;

        try{
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(src), encoding), BUFFER_SIZE);
            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(dest), encoding), BUFFER_SIZE);

            Set<String> contents = new HashSet<String>();
            String line = null;
            while((line = reader.readLine()) != null) {
                contents.add(line); // 去重复
            }

            int count = 0;
            for(String c : contents) {
                if(count++ > 0) {
                    writer.newLine();
                }
                writer.write(c);
            }
        }catch(IOException e) {
            e.printStackTrace();
        } finally {
            if(reader != null) {
                try {
                    reader.close();
                    reader = null;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if(writer != null) {
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
     * @param src 原文件
     * @param dest 新文件
     * @param encoding
     * @param separator
     */
    public static void reverseTwoDomainFileContent(File src, File dest, String encoding, String separator) {
        BufferedReader reader = null;
        BufferedWriter writer = null;

        try{
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(src), encoding), BUFFER_SIZE);
            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(dest), encoding), BUFFER_SIZE);

            String line = null;
            int count = 0;
            while((line = reader.readLine()) != null) {
                String[] ll = line.split(separator);
                if(ll.length > 1) {
                    if(count++ > 0) {
                        writer.newLine();
                    }
                    writer.write(ll[1] + separator + ll[0]);
                }
            }

        }catch(IOException e) {
            e.printStackTrace();
        } finally {
            if(reader != null) {
                try {
                    reader.close();
                    reader = null;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if(writer != null) {
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
     * @param src
     * @param dest
     * @param encoding
     * @param separator
     */
    public static void sortSamllFileContent(File src, File dest, String encoding, String separator) {
        BufferedReader reader = null;
        BufferedWriter writer = null;

        try{
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(src), encoding), BUFFER_SIZE);
            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(dest), encoding), BUFFER_SIZE);

            String line = null;
            SortedSet<String> ss = new TreeSet<String>(new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                    return o1.compareTo(o2);
                }
            });

            while((line = reader.readLine()) != null) {
                ss.add(line);
            }

            Iterator<String> iter = ss.iterator();
            String c = null;
            int count = 0;
            while (iter.hasNext()) {
                c = iter.next();
                if(count++ > 0) {
                    writer.newLine();
                }
                writer.write(c);
            }

        }catch(IOException e) {
            e.printStackTrace();
        } finally {
            if(reader != null) {
                try {
                    reader.close();
                    reader = null;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if(writer != null) {
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
     * 分组文件内容（对同组的分配组号，组号为 UUID）
     * @param src
     * @param dest
     * @param encoding
     * @param separator
     */
    public static void groupKeyKeyStructureFileContent(File src, File dest, String encoding, String separator) {
        BufferedWriter destWriter = null;

        BufferedReader remainingReader = null;
        BufferedWriter remainingWriter = null;

        try{
            if(dest.exists()) {
                dest.delete();
            }
            destWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(dest, true), encoding), BUFFER_SIZE);

            String dir = FilenameUtils.getPath(src.getAbsolutePath());

            // 复制源文件至比对剩余文件
            FileUtils.copyFile(src, new File(dir+"group-tmp1"));
            // 遍历来源文件找到匹配的记录，分配组号并追加至输出文件
            Bag bag = new HashBag();
            String line = null;
            int operateIndex = 0;
            int groupedIndex = 0;
            while(true) {
                remainingReader = new BufferedReader(new InputStreamReader(new FileInputStream((operateIndex % 2) == 0 ? new File(dir+"group-tmp1") : new File(dir+"group-tmp2")), encoding), BUFFER_SIZE);
                remainingWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream((operateIndex % 2) == 0 ? new File(dir+"group-tmp2") : new File(dir+"group-tmp1")), encoding), BUFFER_SIZE);
                operateIndex++;
                int index = 0;
                while((line = remainingReader.readLine()) != null) {
                    String[] ll = line.split(separator);
                    if (ll.length > 1) {
                        KeyKeyBean kk = new KeyKeyBean(ll[0], ll[1], separator);
                        if (index > 0) {
                            // 若大于首个有效行，则判断是否已存在
                            if (containsKeyKey(bag, kk)) {
                                // 排除重复
                                if(!existsKeyKey(bag, kk)) {
                                    bag.add(kk);
                                }
                            } else {
                                // 若不存在，则将数据写入剩余文件
                                if(index > 0) {
                                    remainingWriter.newLine();
                                }
                                remainingWriter.write(line);
                            }
                        } else {
                            // 若为首个有效行，增加第一个数据
                            bag.add(kk);
                        }
                        index++;
                    }
                }

                // 若待分组的文件有效记录存在，则执行下面的操作
                if(index > 0) {
                    // 将分组写入分组文件（目标文件）
                    writeKeyKeysWithUUID(bag, destWriter, separator, groupedIndex > 0 ? false : true);
                    bag.clear();
                    //
                    remainingWriter.flush();
                    groupedIndex++;
                } else {
                    // 分组文件处理完毕，退出循环
                    break;
                }
            }

        }catch(IOException e) {
            e.printStackTrace();
        } finally {
            if(remainingReader != null) {
                try {
                    remainingReader.close();
                    remainingReader = null;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if(remainingWriter != null) {
                try {
                    remainingWriter.flush();
                    remainingWriter.close();
                    remainingWriter = null;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if(destWriter != null) {
                try {
                    destWriter.flush();
                    destWriter.close();
                    destWriter = null;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 判断是为同组记录（根据 Key Key 判断）
     * @param bag
     * @param kk
     * @return
     */
    private static boolean containsKeyKey(Bag bag, KeyKeyBean kk) {
        boolean result = false;
        Iterator iter = bag.iterator();
        while (iter.hasNext()) {
            KeyKeyBean bean = (KeyKeyBean)iter.next();
            if(bean.contains(kk)) {
                result = true;
                break;
            }
        }

        return result;
    }

    /**
     * 判断是为同组记录（根据 Key Key 判断）
     * @param bag
     * @param kk
     * @return
     */
    private static boolean existsKeyKey(Bag bag, KeyKeyBean kk) {
        boolean result = false;
        Iterator iter = bag.iterator();
        while (iter.hasNext()) {
            KeyKeyBean bean = (KeyKeyBean)iter.next();
            if(bean.equals(kk)) {
                result = true;
                break;
            }
        }

        return result;
    }

    /**
     * 将分组记录（集合）分配组号并写入缓冲区
     * @param bag
     * @param writer
     * @param separator
     * @param isFirstGroup
     * @throws IOException
     */
    private static void writeKeyKeysWithUUID(Bag bag, BufferedWriter writer, String separator, boolean isFirstGroup) throws IOException {
        Iterator iter = bag.iterator();
        String uuid = Uuid.getUuid();
        int count = bag.size();
        String gx = count > 1 ? "n" : "1";
        int index = 0;
        while (iter.hasNext()) {
            KeyKeyBean bean = (KeyKeyBean)iter.next();
            if(!isFirstGroup || index > 0) {
                writer.newLine();
            }
            writer.write(gx + separator + uuid + separator + bean.toString());
            index++;
        }
        logger.debug("组号: {}, 共 {} 条记录", uuid, index);
    }

}
