package zw.wormsleep.tools.etl.utils;

import java.util.Properties;

/**
 * Created by wormsleep on 2015/12/30.
 */
public class OperateSystemUtils {
    private static Properties properties = System.getProperties();

    // CPU 数量
    public static int getCpuCount() {
        return Runtime.getRuntime().availableProcessors();
    }

    // JVM 内存 总量
    public static long getTotalMemory() {
        return Runtime.getRuntime().totalMemory();
    }

    // JVM 空闲内存 总量
    public static long getFreeMemory() {
        return Runtime.getRuntime().freeMemory();
    }

    // JVM 最大内存 总量
    public static long getMaxMemory() {
        return Runtime.getRuntime().maxMemory();
    }

    // 系统参数
    public static String getProperty(String prop) {
        return properties.getProperty(prop);
    }

}
