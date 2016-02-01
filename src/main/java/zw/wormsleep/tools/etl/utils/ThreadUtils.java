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
     * @param fSize
     * @param sSize
     * @param power
     * @return
     */
    public static ExecutorService smartThreadPool(int fSize, int sSize, int power) {
        return (Runtime.getRuntime().availableProcessors() * power > fSize * sSize) ? Executors.newCachedThreadPool() : Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * power);
    }
}
