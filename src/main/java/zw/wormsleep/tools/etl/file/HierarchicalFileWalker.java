package zw.wormsleep.tools.etl.file;

import org.apache.commons.collections4.MapIterator;
import org.apache.commons.collections4.map.LinkedMap;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;

public class HierarchicalFileWalker {
    final Logger logger = LoggerFactory.getLogger(HierarchicalFileWalker.class);

    private HashMap<String, String> directories = new HashMap<String, String>();
    private String extension = ".out";

    public HierarchicalFileWalker() {

    }

    /**
     * 设置文件扩展名
     * （默认值 ".out"）
     *
     * @param extension
     */
    public void setExtension(String extension) {
        this.extension = extension;
    }

    /**
     * 检查判断是否为目录
     * （需要重载）
     *
     * @param key
     * @return
     */
    public boolean checkIfDirectory(String key) {
        return false;
    }

    /**
     * 获取上层节点（key）值
     * （需要重载）
     * 例如：有分级文件如下：
     * 42 湖北省
     * 4201 武汉市
     * 420106 武昌区
     * <p>
     * 参数 key = "420106"
     * 返回结果："4201"
     *
     * @param key
     * @return
     */
    public String getUpperKey(String key) {
        return null;
    }

    /**
     * 获取文件（或目录）基础名称
     * （默认格式：value(key)）
     * （可以重载）
     *
     * @param key
     * @param value
     * @return
     */
    public String assembleBaseName(String key, String value) {
        return value + "(" + key + ")";
    }

    /**
     * 处理目录级方法
     * （需要重载）
     *
     * @param directory
     * @param key
     * @param value
     * @param results
     * @return
     */
    public void handleDirectory(File directory, String key, String value,
                                @SuppressWarnings("rawtypes") Collection results) {
    }

    /**
     * 处理文件级方法
     * （需要重载）
     *
     * @param file
     * @param key
     * @param value
     * @param results
     */
    public void handleFile(File file, String key, String value,
                           @SuppressWarnings("rawtypes") Collection results) {
    }

    /**
     * 等级文件遍历器
     *
     * @param startDirectory
     * @param hierarchical
     * @param results
     */
    public void walk(File startDirectory, LinkedMap hierarchical, @SuppressWarnings("rawtypes") Collection results) {

        try {
            // 生成文件的起始目录
            FileUtils.forceMkdir(startDirectory);
            // 分析起始目录的信息
            String basePath = startDirectory.getAbsolutePath();

            MapIterator it = hierarchical.mapIterator();
            while (it.hasNext()) {
                String key = (String) it.next();
                String value = (String) it.getValue();
                String baseName = assembleBaseName(key, value);
                String upperPath = directories.get(getUpperKey(key));
                // 组装待生成文件或目录的父目录
                String parentPath = (upperPath != null ? upperPath : basePath) + "/";
                if (checkIfDirectory(key)) {
                    // 生成目录
                    File directory = new File(parentPath + baseName);
                    FileUtils.forceMkdir(directory);
                    // 目录路径注册
                    String dirPath = directory.getAbsolutePath();
                    logger.info("@@@ 创建目录: {}", dirPath);
                    directories.put(key, dirPath);
                    // 处理目录级事件
                    handleDirectory(directory, key, value, results);
                } else {
                    // 生成文件
                    File file = new File(parentPath + baseName + extension);
                    // 处理文件级事件
                    handleFile(file, key, value, results);
                }
            }

        } catch (IOException e) {
            logger.error("@@@ 文件处理异常!", e);
        }
    }
}
