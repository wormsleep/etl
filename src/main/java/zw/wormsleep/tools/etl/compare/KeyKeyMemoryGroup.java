package zw.wormsleep.tools.etl.compare;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zw.wormsleep.tools.etl.utils.Uuid;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by wormsleep on 2016/1/13.
 */
public class KeyKeyMemoryGroup {
    final Logger logger = LoggerFactory.getLogger(KeyKeyMemoryGroup.class);

    private File src;
    private String encoding;
    private String separator;
    private int splitSize;

    private KeyKey[] sortedKeyKeysByKey1; // 已按 key1 值排序的汇总待分组数组
    private Collection<KeyKey[]> sortedKey1Arrays; // 已按 key1 值排序的分段待分组列表（检索用）
    private Collection<KeyKey[]> sortedKey2Arrays; // 已按 key2 值排序的分段待分组列表（检索用）

    public KeyKeyMemoryGroup(File src, String encoding, String separator, int splitSize) {
        this.src = src;
        this.encoding = encoding;
        this.separator = separator;
        this.splitSize = splitSize < 10000 ? 10000 : splitSize;

        init();
    }

    /**
     * 分组处理 - 主方法
     *
     * @param dest 分组目标文件
     * @throws IOException
     */
    public void group(File dest) throws IOException {
        long startTime = System.currentTimeMillis();
        // 最大处理次数
        int totalCount = sortedKeyKeysByKey1.length;
        // 分组的数据 - 最终已分组数据
        List<String> groupedKeyKeys = new ArrayList<String>();
        // 防呆处理
        if (totalCount < 1) {
            FileUtils.writeLines(dest, encoding, new ArrayList<String>() {{
                add("");
            }});
            return;
        }
        // 处理时 - 下标
        int currentIndex = 0;
        // 单次分组数据集
        Set<KeyKey> oneGroupedKeyKeys;
        // 临时参数
        String key1, nextKey1;
        while (true) {

            oneGroupedKeyKeys = new HashSet<KeyKey>();

            // 若遇到已经分组的跳过
            if (sortedKeyKeysByKey1[currentIndex].isGrouped()) {
                // 若未超出下标，则递增下标
                if (currentIndex < totalCount) {
                    currentIndex++;
                }
            } else {

                // 新分组的首个 key1 值
                key1 = sortedKeyKeysByKey1[currentIndex].getKey1();

                do {
                    oneGroupedKeyKeys.add(sortedKeyKeysByKey1[currentIndex++]);
                    nextKey1 = (currentIndex < totalCount) ?
                            sortedKeyKeysByKey1[currentIndex].getKey1() :
                            null;
                } while (key1.equals(nextKey1));


                // 合并检索并分组
                mergeSearch(oneGroupedKeyKeys);
                // 将本次分组涉及的对象在总队列中打标
                markGroupedKeyKeys(oneGroupedKeyKeys);
                // 将本次分组结果增加到总分组集中
                fillGroupedKeyKeys(groupedKeyKeys, oneGroupedKeyKeys);
            }

            // 循环跳出条件
            if (currentIndex > totalCount - 1) {
                break;
            }
        }

        // 将所有的分组结果写入文件
        FileUtils.writeLines(dest, encoding, groupedKeyKeys);

        long endTime = System.currentTimeMillis();
        long consuming = (endTime - startTime) / 1000;
        logger.info("@@@ （自然）分组任务总耗时 : {} ", (consuming / 60) > 0 ? (String.valueOf(consuming / 60) + " 分钟") : "小于 1 分钟 (约为 " + String.valueOf(consuming % 60) + " 秒)");

    }

    // 合并检索并分组 - 建议用多线程（但要考虑变量共享的问题）
    private void mergeSearch(Collection<KeyKey> oneGroupKeyKeys) {

        // 定义 key1 和 key2 排重列表
        Set<String> uniqueKey1s = new LinkedHashSet<String>();
        Set<String> uniqueKey2s = new LinkedHashSet<String>();

        for (KeyKey keyKey : oneGroupKeyKeys) {
            uniqueKey1s.add(keyKey.getKey1());
            uniqueKey2s.add(keyKey.getKey2());
        }

        // 记录 key1 和 key2 键值排重原始总数
        int originKey1sSize = uniqueKey1s.size();
        int originKey2sSize = uniqueKey2s.size();

        int newKey1sSize;
        int newKey2sSize = originKey2sSize;

        // 检索 key1 和 key2
        String[] untreated;
        Collection<KeyKey> foundKeyKeys;
        int mergeSearchCount = 0;
        do {
            // 检索包含 key2 集合的所有 keykey 对象
            untreated = mergeSearchCount > 0 ? Arrays.copyOfRange(uniqueKey2s.toArray(new String[newKey2sSize]),
                    originKey2sSize, newKey2sSize) : uniqueKey2s.toArray(new String[originKey2sSize]);
            foundKeyKeys = searchKeys(sortedKey2Arrays, untreated, KeyKey.key2Comparator());
            oneGroupKeyKeys.addAll(foundKeyKeys);

            for (KeyKey keyKey : foundKeyKeys) {
                uniqueKey1s.add(keyKey.getKey1());
            }

            newKey1sSize = uniqueKey1s.size();

            // 若 key1 集合有新增的，则检索包含新增 key1 集合的所有 keykey 对象
            if (newKey1sSize > originKey1sSize) {
                untreated = Arrays.copyOfRange(uniqueKey1s.toArray(new String[newKey1sSize]),
                        originKey1sSize, newKey1sSize);
                foundKeyKeys = searchKeys(sortedKey1Arrays, untreated, KeyKey.key1Comparator());
                oneGroupKeyKeys.addAll(foundKeyKeys);

                for (KeyKey keyKey : foundKeyKeys) {
                    uniqueKey2s.add(keyKey.getKey2());
                }

                newKey2sSize = uniqueKey2s.size();
            }

            mergeSearchCount++;

        } while (newKey2sSize > originKey2sSize && mergeSearchCount < 10000); // 设定防呆阀值 10000（由于单户匹配的总数估计不可能超过该阀值）

    }

    // 检索 key 值对应的 keykey 对象集 - 多线程
    // 经过测试发现这种方法比较慢 - 正在郁闷中。。。
    private Collection<KeyKey> searchKeys2(Collection<KeyKey[]> sortedKeyArrays, String[] keys, Comparator<KeyKey> comparator) {
        Vector<KeyKey> foundKeyKeys = new Vector<KeyKey>();

        // 通过 String[] keys 重新构造 KeyKey[] kks
        int keySize = keys.length;
        KeyKey[] kks = new KeyKey[keySize];
        for (int i = 0; i < keySize; i++) {
            kks[i] = new KeyKey(keys[i], keys[i], separator);
        }
        ExecutorService pool = Executors.newCachedThreadPool();
        // 将每个 key 在每个分片数组中检索
//        logger.info("@@@ 建立多线程池。。。");

        for (KeyKey key : kks) {
            for (KeyKey[] sortedKeys : sortedKeyArrays) {
                pool.execute(new KeyKeySearchMemoryThread(foundKeyKeys, sortedKeys, key, comparator));
            }
        }

        pool.shutdown();

        while (true) {
            if (pool.isTerminated()) {
//                logger.debug("@@@ 单次分组（多线程）完成...准备返回");
                break;
            }
        }

        return foundKeyKeys;
    }

    // 检索 key 值对应的 keykey 对象集
    private Collection<KeyKey> searchKeys(Collection<KeyKey[]> sortedKeyArrays, String[] keys, Comparator<KeyKey> comparator) {
        Vector<KeyKey> foundKeyKeys = new Vector<KeyKey>();

        // 通过 String[] keys 重新构造 KeyKey[] kks
        int keySize = keys.length;
        KeyKey[] kks = new KeyKey[keySize];
        for (int i = 0; i < keySize; i++) {
            kks[i] = new KeyKey(keys[i], keys[i], "-");
        }

        // 将每个 key 在每个分片数组中检索
        for (KeyKey key : kks) {
            for (KeyKey[] sortedKeys : sortedKeyArrays) {
                int position = Arrays.binarySearch(sortedKeys, key, comparator);
                // 若找到了再向左找和向右找
                if (position >= 0) {
                    // 这是找到的任一条
                    foundKeyKeys.add(sortedKeys[position]);
                    // 基于该条数据的位置向左向右找
                    int arraySize = sortedKeys.length;
                    boolean leftRemaining = true;
                    boolean rightRemaining = true;

                    int leftPosition = position > 0 ? position - 1 : 0;
                    int rightPosition = position < arraySize - 1 ? position + 1 : arraySize;
                    do {

                        if (leftRemaining) {
                            if (leftPosition < 1) {
                                leftRemaining = false;
                            } else {
                                if (comparator.compare(key, sortedKeys[leftPosition]) == 0) {
                                    foundKeyKeys.add(sortedKeys[leftPosition--]);
                                } else {
                                    leftRemaining = false;
                                }
                            }
                        }

                        if (rightRemaining) {
                            if (rightPosition < arraySize) {
                                if (comparator.compare(key, sortedKeys[rightPosition]) == 0) {
                                    foundKeyKeys.add(sortedKeys[rightPosition++]);
                                } else {
                                    rightRemaining = false;
                                }
                            } else {
                                rightRemaining = false;
                            }
                        }

                    } while (leftRemaining || rightRemaining);
                }
            }
        }

        return foundKeyKeys;
    }

    // 将本次分组涉及的对象在总队列中打标
    private void markGroupedKeyKeys(Collection<KeyKey> oneGroupKeyKeys) {
        for (KeyKey keyKey : oneGroupKeyKeys) {
            sortedKeyKeysByKey1[keyKey.getIndex()].setGrouped(true);
        }
    }

    // 将本次分组结果增加到总分组集中
    private void fillGroupedKeyKeys(Collection<String> groupedKeyKeys, Collection<KeyKey> oneGroupKeyKeys) {
        String uuid = Uuid.getUuid();
        String multiRecord = oneGroupKeyKeys.size() > 1 ? "n" : "1";
        for (KeyKey keykey : oneGroupKeyKeys) {
            groupedKeyKeys.add(multiRecord + separator + uuid + separator + keykey.toString());
        }
    }

    // 初始化处理
    private void init() {
        try {
            // 将 key key 结构的文件读入 List
            List<String> lines = FileUtils.readLines(src, encoding);

            // 初始化 keykey 数组对象
            sortedKeyKeysByKey1 = new KeyKey[lines.size()];

            String[] ls;
            int lineIndex = 0;
            for (String line : lines) {
                ls = line.split(separator);
                if (ls.length > 1) {
                    sortedKeyKeysByKey1[lineIndex] = new KeyKey(ls[0], ls[1], separator);
                    lineIndex++;
                }
            }

            // 目前必须要这样处理 - 过滤空数据行或无效数 （否则对于数组中的空对象会出现 Arrays.mergeSort 方法内部错误）
            // 还好对于效率没太大影响，运行内存设置为 1024 时可以处理至少 100 万数据，100 万数据的处理为 5 秒
            sortedKeyKeysByKey1 = Arrays.copyOf(sortedKeyKeysByKey1, lineIndex);

            // 排序数组对象 - 规则：按 key1 值排序
            Arrays.sort(sortedKeyKeysByKey1, new Comparator<KeyKey>() {
                @Override
                public int compare(KeyKey o1, KeyKey o2) {
                    return o1.getKey1().compareTo(o2.getKey1());
                }
            });

            // *** 初始化每个 keykey 对象 index 值 - 该值作为已排序数组的原始下标值
            for (int i = 0; i < sortedKeyKeysByKey1.length; i++) {
                sortedKeyKeysByKey1[i].setIndex(i);
            }

            // 对已进行 key1 值排序的数组进行拆分
            sortedKey1Arrays = splitKeyKeys(sortedKeyKeysByKey1);

            // 已按 key2 值排序的汇总待分组数组
            KeyKey[] sortedKeyKeysByKey2;
            // *** 拷贝已排序并已生成原始 index 的数组
            sortedKeyKeysByKey2 = Arrays.copyOf(sortedKeyKeysByKey1, sortedKeyKeysByKey1.length);

            // 排序数组对象 - 规则：按 key2 值排序
            Arrays.sort(sortedKeyKeysByKey2, new Comparator<KeyKey>() {
                @Override
                public int compare(KeyKey o1, KeyKey o2) {
                    return o1.getKey2().compareTo(o2.getKey2());
                }
            });

            // 对已进行 key1 值排序的数组进行拆分
            sortedKey2Arrays = splitKeyKeys(sortedKeyKeysByKey2);

            logger.info("@@@ 自然分组初始化...\n" +
                            "源文件 {} 共计 {} 行待分组记录\n" +
                            "已对源文件进行 KEY1 排序并分组，其分组最大 {} 行共计 {} 个分组\n" +
                            "已对源文件进行 KEY2 排序并分组，其分组最大 {} 行共计 {} 个分组\n",
                    src.getAbsolutePath(), sortedKeyKeysByKey1.length,
                    splitSize, sortedKey1Arrays.size(),
                    splitSize, sortedKey2Arrays.size());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 拆分数组
    private List<KeyKey[]> splitKeyKeys(KeyKey[] arrays) {
        List<KeyKey[]> kks = new ArrayList<KeyKey[]>();

        int size = arrays.length;

        int from = 0, to;
        int count = (size > splitSize) ? ((size / splitSize) + (size % splitSize > 0 ? 1 : 0)) : 1;
        for (int i = 0; i < count; i++) {
            to = (from + splitSize) > size ? size : (from + splitSize);
            kks.add(Arrays.copyOfRange(arrays, from, to));
            from = to;
        }

        return kks;
    }

}
