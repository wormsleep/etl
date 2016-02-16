package zw.wormsleep.tools.etl.utils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by wormsleep on 2016/1/19.
 */
public class ThreadUtils {
    /**
     * 根据操作系统 CPU 数量和比对文件数量乘积判定生成的线程池（类型）
     *
     * @param fSize 首文件分割总数
     * @param sSize 次文件分割总数
     * @param power 倍数
     * @return
     */
    public static ExecutorService smartSimilarityThreadPool(int fSize, int sSize, int power) {
        return (Runtime.getRuntime().availableProcessors() * power > fSize * sSize) ? Executors.newCachedThreadPool() : Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * power);
    }

    /**
     * 生成供批量文件排序使用的线程池（类型）
     * @param fSize 文件分割总数
     * @param power 倍数
     * @return
     */
    public static ExecutorService smartSortThreadPool(int fSize, int power) {
//        Runtime.getRuntime().freeMemory();
        return Executors.newCachedThreadPool();
    }
}
