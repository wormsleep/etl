package zw.wormsleep.tools.etl.utils;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.SubnodeConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.lang3.text.translate.UnicodeUnescaper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ConfigParserUtils {
    static final Logger logger = LoggerFactory.getLogger(ConfigParserUtils.class);

    static final String DEFAULT_CONFIG_FILENAME = "etl-config.xml";
    static final String NODE_RESOURCE = "resource";
    static final String PROP_BUSINESS_TYPE = "[@businesstype]";
    static final String NODE_COLUMNS_COLUMN = "columns.column";
    static final String NODE_DATABASE = "database";

    /**
     * 获取默认的配置文件
     *
     * @return
     */
    public static File getDefaultConfigurationFile() {
        try {
            XMLConfiguration configuration = new XMLConfiguration(DEFAULT_CONFIG_FILENAME);
            return configuration.getFile();
        } catch (ConfigurationException e) {
            logger.error("@@@ 未找到配置文件！", e);
        }
//        String path = ConfigParserUtils.class.getClassLoader().getResource(DEFAULT_CONFIG_FILENAME).getPath();
        return null;
    }

    /**
     * 获取指定数据库 c3p0 配置对象信息
     *
     * @param database
     * @return
     * @throws ConfigurationException
     */
    public static Map<String, String> getDatabaseConfiguration(String database)
            throws ConfigurationException {
        return getDatabaseConfiguration(database, getDefaultConfigurationFile());
    }

    /**
     * 是否存在指定节点。
     * 注意该节点必须有值
     *
     * @param key
     * @param configuration
     * @return
     */
    public static boolean containsKey(String key, File configuration) {
        try {
            XMLConfiguration config = new XMLConfiguration(configuration);

            return config.containsKey(key);
        } catch (ConfigurationException e) {
            return false;
        }

    }

    /**
     * 是否存在指定节点
     * 注意该节点必须有值
     *
     * @param key
     * @return
     */
    public static boolean containsKey(String key) {
        return containsKey(key, getDefaultConfigurationFile());

    }

    /**
     * 获取指定数据库 c3p0 配置对象信息
     *
     * @param database
     * @param configuration
     * @return
     * @throws ConfigurationException
     */
    public static Map<String, String> getDatabaseConfiguration(String database,
                                                               File configuration) throws ConfigurationException {
        Map<String, String> dababaseConfiguration = new HashMap<String, String>();

        XMLConfiguration config = new XMLConfiguration(configuration);

        if (database != null && !database.equals("")) {
            SubnodeConfiguration databaseSubNode = config
                    .configurationAt(NODE_DATABASE + "." + database);
            Iterator<String> iter = databaseSubNode.getKeys();
            while (iter.hasNext()) {
                String key = iter.next();
                dababaseConfiguration.put(key, databaseSubNode.getString(key));
            }
        }

        return dababaseConfiguration;
    }

    /**
     * 获取指定业务配置信息
     *
     * @param businessType
     * @return
     * @throws ConfigurationException
     */
    public static HierarchicalConfiguration getResourceConfiguration(
            String businessType) throws ConfigurationException {
        return getResourceConfiguration(businessType, getDefaultConfigurationFile());
    }

    /**
     * 获取指定业务配置信息
     *
     * @param businessType
     * @param configuration
     * @return
     * @throws ConfigurationException
     */
    public static HierarchicalConfiguration getResourceConfiguration(
            String businessType, File configuration)
            throws ConfigurationException {
        HierarchicalConfiguration business = null;

        XMLConfiguration config = new XMLConfiguration(configuration);
        List<HierarchicalConfiguration> resources = config
                .configurationsAt(NODE_RESOURCE);
        // 获取指定 type 属性的节点
        for (HierarchicalConfiguration resource : resources) {
            if (resource.getString(PROP_BUSINESS_TYPE) != null
                    && resource.getString(PROP_BUSINESS_TYPE).equalsIgnoreCase(
                    businessType)) {
                business = resource;
                break;
            }
        }

        if (business != null) {
            logger.debug("@@@ 已获取 ETL 配置文件指定业务({})节点!", businessType);
        } else {
            logger.error("@@@ 未获取 ETL 配置文件指定业务({})节点!", businessType);
        }

        return business;
    }

    /**
     * 获取指定数据库配置信息
     *
     * @param database
     * @return
     * @throws ConfigurationException
     */
    public static HierarchicalConfiguration getDatabaseHierarchicalConfiguration(
            String database) throws ConfigurationException {
        return getDatabaseHierarchicalConfiguration(database, getDefaultConfigurationFile());
    }


    /**
     * 获取指定数据库配置信息
     *
     * @param database
     * @param configuration
     * @return
     * @throws ConfigurationException
     */
    public static HierarchicalConfiguration getDatabaseHierarchicalConfiguration(
            String database, File configuration)
            throws ConfigurationException {
        HierarchicalConfiguration databaseSubNode = null;

        XMLConfiguration config = new XMLConfiguration(configuration);
        if (database != null && !database.equals("")) {
            databaseSubNode = config
                    .configurationAt(NODE_DATABASE + "." + database);
        }

        return databaseSubNode;
    }

    /**
     * 获取指定业务列配置信息
     *
     * @param business
     * @return
     */
    public static List<HierarchicalConfiguration> getColumnConfiguration(
            HierarchicalConfiguration business) {
        return getConfigurationList(business, NODE_COLUMNS_COLUMN);
    }

    /**
     * 获取指定节点配置列表
     *
     * @param parent
     * @param node
     * @return
     */
    public static List<HierarchicalConfiguration> getConfigurationList(
            HierarchicalConfiguration parent, String node) {
        return parent.configurationsAt(node);
    }

    /**
     * 对分隔符进行 Unicode 至 UTF-8 编码解析
     *
     * @param value
     * @return
     */
    public static String getSeparator(String value) {
        String separator = "\t";

        if (value != null && !value.equals("")) {
            if (value.equalsIgnoreCase("tab")) {
                separator = "\t";
            } else if (value.startsWith("\\")) {
                UnicodeUnescaper uu = new UnicodeUnescaper();
                separator = uu.translate(value);
            } else {
                separator = value;
            }
        }

        return separator;
    }


}
