package zw.wormsleep.tools.etl.utils;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.HierarchicalConfiguration.Node;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.tree.ConfigurationNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zw.wormsleep.tools.etl.database.DatabaseHelper;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.Types;
import java.util.List;
import java.util.Map;

public class ConfigBuilderUtils {
    static final Logger logger = LoggerFactory
            .getLogger(ConfigBuilderUtils.class);

    /**
     * 创建数据库表对表拷贝配置文件
     * <p/>
     * 1、支持目标表名自定
     * 2、支持目标表字段自定
     */
    public static void createTable2TableConfiguration(File file,
                                                      String srcDatabase, String srcDatabaseType, String destDatabase,
                                                      String destDatabaseType, String srcTableName, String select, String destTableName, ConfigurationNode columns, boolean srcTableColumnNameToLowerCase)
            throws ConfigurationException {
        XMLConfiguration config = new XMLConfiguration();
        config.setRootElementName("etl");
        ConfigurationNode root = config.getRootNode();

        ConfigurationNode databaseNode = buildNode("database");
        HierarchicalConfiguration srcDatabaseConfig = ConfigParserUtils
                .getDatabaseHierarchicalConfiguration(srcDatabase);
        HierarchicalConfiguration destDatabaseConfig = ConfigParserUtils
                .getDatabaseHierarchicalConfiguration(destDatabase);

        databaseNode.addChild(convertToConfigurationNode(srcDatabaseConfig));
        databaseNode.addChild(convertToConfigurationNode(destDatabaseConfig));

        ConfigurationNode resource = new Node("resource");
        resource.addAttribute(buildAttributeNode("businesstype", srcTableName));

        ConfigurationNode input = buildNode("input", "type", "database");
        input.addChild(buildNode("database", srcDatabase, "type",
                srcDatabaseType));
        input.addChild(buildNode("sql", select));
        input.addChild(buildNode("columnnametolowercase", srcTableColumnNameToLowerCase));

        ConfigurationNode output = buildNode("output", "type", "database");
        output.addChild(buildNode("database", destDatabase, "type",
                destDatabaseType));
        output.addChild(buildNode("table", destTableName));

        resource.addChild(input);
        resource.addChild(output);
        resource.addChild(columns);

        root.addChild(resource);

        root.addChild(databaseNode);

        config.save(file);
    }


    /**
     * 创建数据库表对表批量拷贝配置文件
     *
     * @param file
     * @return
     * @throws ConfigurationException
     */
    public static void createTables2TablesConfiguration(File file,
                                                        String srcDatabase, String srcDatabaseType, String destDatabase,
                                                        String destDatabaseType, List<String> tables)
            throws ConfigurationException {
        XMLConfiguration config = new XMLConfiguration();
        config.setRootElementName("etl");
        ConfigurationNode root = config.getRootNode();

        ConfigurationNode databaseNode = buildNode("database");
        HierarchicalConfiguration srcDatabaseConfig = ConfigParserUtils
                .getDatabaseHierarchicalConfiguration(srcDatabase);
        HierarchicalConfiguration destDatabaseConfig = ConfigParserUtils
                .getDatabaseHierarchicalConfiguration(destDatabase);

        databaseNode.addChild(convertToConfigurationNode(srcDatabaseConfig));
        databaseNode.addChild(convertToConfigurationNode(destDatabaseConfig));

        for (String tableName : tables) {
            ConfigurationNode resource = new Node("resource");
            resource.addAttribute(buildAttributeNode("businesstype", tableName));

            ConfigurationNode input = buildNode("input", "type", "database");
            input.addChild(buildNode("database", srcDatabase, "type",
                    srcDatabaseType));
            input.addChild(buildNode("table", tableName));

            ConfigurationNode output = buildNode("output", "type", "database");
            output.addChild(buildNode("database", destDatabase, "type",
                    destDatabaseType));
            output.addChild(buildNode("table", tableName));

            resource.addChild(input);
            resource.addChild(output);

            root.addChild(resource);
        }

        root.addChild(databaseNode);

        config.save(file);
    }

    /**
     * 创建属性对象
     *
     * @param name
     * @param value
     * @return
     */
    public static ConfigurationNode buildAttributeNode(String name, Object value) {
        ConfigurationNode attribute = new Node(name, value);
        attribute.setAttribute(true);

        return attribute;
    }

    /**
     * 创建节点对象 - 仅有名称
     *
     * @param name
     * @return
     */
    public static ConfigurationNode buildNode(String name) {
        return new Node(name);
    }

    /**
     * 创建节点对象 - 名称/值
     *
     * @param name
     * @param value
     * @return
     */
    public static ConfigurationNode buildNode(String name, Object value) {
        return new Node(name, value);
    }

    /**
     * 创建节点对象 - 仅有名称带单个属性
     *
     * @param name
     * @param attrName
     * @param attrValue
     * @return
     */
    public static ConfigurationNode buildNode(String name, String attrName,
                                              Object attrValue) {
        ConfigurationNode node = buildNode(name);
        node.addAttribute(buildAttributeNode(attrName, attrValue));
        return node;
    }

    /**
     * 创建节点对象 - 名称/值带单个属性
     *
     * @param name
     * @param attrName
     * @param attrValue
     * @return
     */
    public static ConfigurationNode buildNode(String name, Object value,
                                              String attrName, Object attrValue) {
        ConfigurationNode node = buildNode(name, value);
        node.addAttribute(buildAttributeNode(attrName, attrValue));
        return node;
    }

    /**
     * 创建节点对象 - 仅有名称带多个属性
     *
     * @param name
     * @param attributes
     * @return
     */
    public static ConfigurationNode buildNode(String name,
                                              Map<String, Object> attributes) {
        ConfigurationNode node = buildNode(name);
        for (String key : attributes.keySet()) {
            node.addAttribute(buildAttributeNode(key, attributes.get(key)));
        }
        return node;
    }

    /**
     * 创建节点对象 - 名称带多个属性
     *
     * @param name
     * @param value
     * @param attributes
     * @return
     */
    public static ConfigurationNode buildNode(String name, String value,
                                              Map<String, Object> attributes) {
        ConfigurationNode node = buildNode(name, value);
        for (String key : attributes.keySet()) {
            node.addAttribute(buildAttributeNode(key, attributes.get(key)));
        }
        return node;
    }

    /**
     * 将继承配置转换至配置文件节点
     *
     * @param config
     * @return
     */
    public static ConfigurationNode convertToConfigurationNode(
            HierarchicalConfiguration config) {

        ConfigurationNode rootNode = config.getRootNode();
        String rootNodeName = rootNode.getName();
        int childrenCount = rootNode.getChildrenCount();
        int attributeCount = rootNode.getAttributeCount();

        ConfigurationNode resultNode = buildNode(rootNodeName);

        if (attributeCount > 0) {
            for (ConfigurationNode attrNode : rootNode.getAttributes()) {
                resultNode.addAttribute(buildAttributeNode(attrNode.getName(),
                        attrNode.getValue()));
            }
        }

        if (childrenCount > 0) {
            for (ConfigurationNode childNode : rootNode.getChildren()) {
                resultNode.addChild(buildNode(childNode.getName(),
                        childNode.getValue()));
            }
        }

        return resultNode;

    }

    /**
     * 将配置节点内容转换成字符串
     *
     * @param node
     */
    public static String convertNodeToString(ConfigurationNode node) {
        String result = null;
        XMLConfiguration config = new XMLConfiguration();
        config.setRootElementName("etl");
        ConfigurationNode root = config.getRootNode();
        root.addChild(node);

        Writer writer = new StringWriter();
        try {
            config.save(writer);
            result = writer.toString();
        } catch (ConfigurationException e) {
            logger.error("处理异常：" + e.getMessage());
        } finally {
            try {
                writer.close();
                writer = null;
            } catch (IOException e) {
                logger.error("处理异常：" + e.getMessage());
            }
        }

        return result;
    }

    /**
     * 辅助类 - 通过分析来源数据或目标数据自动生成配置文件
     *
     * @author zhaowei
     */
    public static class Assistant {
        /**
         * 分析器定义
         */
        public interface Analyst {
            /**
             * 构造配置文件中的 columns 节点内容, 以便快速填写配置文件
             *
             * @param type 生成类型 - 0: 仅 field 节点; 1: 带 index 节点; 2: 带 name 节点
             * @return
             */
            public ConfigurationNode buildColumnsNode(int type);
        }

        /**
         * 列名过滤器
         */
        public interface FilterName {
            /**
             * 过滤
             *
             * @param name
             * @return
             */
            public boolean accept(String name);
        }

        /**
         * SQL 类数据分析器 - 构造配置文件中的 columns 节点及子节点
         *
         * @param database  数据库配置节点名称
         * @param tableName 表名
         * @return Analyst 接口实现对象
         */
        public static Analyst getSQLAnalyst(final String database,
                                            final String tableName) {
            return new Analyst() {

                @Override
                public ConfigurationNode buildColumnsNode(int type) {
                    ConfigurationNode columnsNode = buildNode("columns");
                    logger.debug("@@@ 组装 columns 节点配置 ...");
                    StringBuffer debugStr = new StringBuffer();
                    List<String> columns = DatabaseHelper.getTableColumnsName(database, tableName);

                    ConfigurationNode columnNode = null;
                    ConfigurationNode fieldNode = null;
                    ConfigurationNode indexNode = null;
                    ConfigurationNode nameNode = null;

                    debugStr.append("<columns>\n");
                    int columnCount = columns.size();
                    for (int i = 0; i < columnCount; i++) {
                        String fieldName = columns.get(i);
                        fieldNode = buildNode("field", fieldName);
                        columnNode = buildNode("column");
                        debugStr.append("\t<column>\n");

                        // 根据 type 值判定生成内容
                        if (type == 1) {
                            indexNode = buildNode("index", i);
                            columnNode.addChild(indexNode);
                            debugStr.append("\t\t<index>" + i + "</index>\n");
                        } else if (type == 2) {
                            nameNode = buildNode("name", "ExcelColumnName");
                            columnNode.addChild(nameNode);
                            debugStr.append("\t\t<name>ExcelColumnName</name>\n");
                        }

                        columnNode.addChild(fieldNode);
                        debugStr.append("\t\t<field>" + fieldName + "</field>\n");
                        columnsNode.addChild(columnNode);
                        debugStr.append("\t</column>\n");
                    }

                    // 带列头的增加其 header 属性且值为 true
                    if (type == 2) {
                        columnsNode.addAttribute(buildAttributeNode("header", "true"));
                    }

                    debugStr.append("</columns>");
                    logger.debug("@@@ 输出...\n{}", debugStr.toString());
                    return columnsNode;
                }
            };
        }

        /**
         * SQL 类数据分析器 - 构造配置文件中的 columns 节点及子节点
         *
         * @param database  数据库配置节点名称
         * @param tableName 表名
         * @param filter    列名过滤器
         * @return Analyst 接口实现对象
         */
        public static Analyst getSQLAnalyst(final String database,
                                            final String tableName, final FilterName filter) {
            return new Analyst() {

                @Override
                public ConfigurationNode buildColumnsNode(int type) {
                    ConfigurationNode columnsNode = buildNode("columns");
                    logger.debug("@@@ 组装 columns 节点配置 ...");
                    StringBuffer debugStr = new StringBuffer();
                    List<String> columns = DatabaseHelper.getTableColumnsName(database, tableName);

                    ConfigurationNode columnNode = null;
                    ConfigurationNode fieldNode = null;
                    ConfigurationNode indexNode = null;
                    ConfigurationNode nameNode = null;

                    debugStr.append("<columns>\n");
                    int columnCount = columns.size();
                    for (int i = 0; i < columnCount; i++) {
                        String fieldName = columns.get(i);

                        if (filter.accept(fieldName)) {

                            fieldNode = buildNode("field", fieldName);
                            columnNode = buildNode("column");
                            debugStr.append("\t<column>\n");

                            // 根据 type 值判定生成内容
                            if (type == 1) {
                                indexNode = buildNode("index", i);
                                columnNode.addChild(indexNode);
                                debugStr.append("\t\t<index>" + i + "</index>\n");
                            } else if (type == 2) {
                                nameNode = buildNode("name", "ExcelColumnName");
                                columnNode.addChild(nameNode);
                                debugStr.append("\t\t<name>ExcelColumnName</name>\n");
                            }

                            columnNode.addChild(fieldNode);
                            debugStr.append("\t\t<field>" + fieldName + "</field>\n");
                            columnsNode.addChild(columnNode);
                            debugStr.append("\t</column>\n");
                        }
                    }

                    // 带列头的增加其 header 属性且值为 true
                    if (type == 2) {
                        columnsNode.addAttribute(buildAttributeNode("header", "true"));
                    }

                    debugStr.append("</columns>");
                    logger.debug("@@@ 输出...\n{}", debugStr.toString());
                    return columnsNode;
                }
            };
        }

        /**
         * SQL 类数据分析器 - 构造配置文件中的 columns 节点及子节点
         * 根据数据库表列元数据
         *
         * @param columns 表列元数据对象
         * @return
         */
        public static Analyst getSQLAnalyst(final Map<String, Map<String, Object>> columns) {
            return new Analyst() {

                @Override
                public ConfigurationNode buildColumnsNode(int type) {
                    ConfigurationNode columnsNode = buildNode("columns");
                    logger.debug("@@@ 组装 columns 节点配置 ...");
                    StringBuffer debugStr = new StringBuffer();

                    ConfigurationNode columnNode = null;
                    ConfigurationNode fieldNode = null;
                    ConfigurationNode indexNode = null;
                    ConfigurationNode nameNode = null;

                    debugStr.append("<columns>\n");
                    int columnCount = 0;
                    Map<String, Object> attributes;
                    for (String columnName : columns.keySet()) {
                        attributes = columns.get(columnName);
                        fieldNode = buildNode("field", columnName);
                        columnNode = buildNode("column");
                        addColumnNodeAttributes(columnNode, attributes, debugStr);

                        // 根据 type 值判定生成内容
                        if (type == 1) {
                            indexNode = buildNode("index", columnCount);
                            columnNode.addChild(indexNode);
                            debugStr.append("\t\t<index>" + columnCount + "</index>\n");
                        } else if (type == 2) {
                            nameNode = buildNode("name", "ExcelColumnName");
                            columnNode.addChild(nameNode);
                            debugStr.append("\t\t<name>ExcelColumnName</name>\n");
                        }

                        columnNode.addChild(fieldNode);
                        debugStr.append("\t\t<field>" + columnName + "</field>\n");
                        columnsNode.addChild(columnNode);
                        debugStr.append("\t</column>\n");

                        columnCount++;
                    }

                    // 带列头的增加其 header 属性且值为 true
                    if (type == 2) {
                        columnsNode.addAttribute(buildAttributeNode("header", "true"));
                    }

                    debugStr.append("</columns>");
                    logger.debug("@@@ 输出...\n{}", debugStr.toString());
                    return columnsNode;
                }
            };
        }

        /**
         * SQL 类数据分析器 - 构造配置文件中的 columns 节点及子节点
         *
         * @param columns 表列元数据对象
         * @param filter  列名过滤器
         * @return
         */
        public static Analyst getSQLAnalyst(final Map<String, Map<String, Object>> columns, final FilterName filter) {
            return new Analyst() {

                @Override
                public ConfigurationNode buildColumnsNode(int type) {
                    ConfigurationNode columnsNode = buildNode("columns");
                    logger.debug("@@@ 组装 columns 节点配置 ...");
                    StringBuffer debugStr = new StringBuffer();

                    ConfigurationNode columnNode = null;
                    ConfigurationNode fieldNode = null;
                    ConfigurationNode indexNode = null;
                    ConfigurationNode nameNode = null;

                    debugStr.append("<columns>\n");
                    int columnCount = 0;
                    Map<String, Object> attributes;
                    for (String columnName : columns.keySet()) {

                        if (filter.accept(columnName)) {

                            attributes = columns.get(columnName);
                            fieldNode = buildNode("field", columnName);
                            columnNode = buildNode("column");
                            addColumnNodeAttributes(columnNode, attributes, debugStr);

                            // 根据 type 值判定生成内容
                            if (type == 1) {
                                indexNode = buildNode("index", columnCount);
                                columnNode.addChild(indexNode);
                                debugStr.append("\t\t<index>" + columnCount + "</index>\n");
                            } else if (type == 2) {
                                nameNode = buildNode("name", "ExcelColumnName");
                                columnNode.addChild(nameNode);
                                debugStr.append("\t\t<name>ExcelColumnName</name>\n");
                            }

                            columnNode.addChild(fieldNode);
                            debugStr.append("\t\t<field>" + columnName + "</field>\n");
                            columnsNode.addChild(columnNode);
                            debugStr.append("\t</column>\n");
                        }
                    }

                    // 带列头的增加其 header 属性且值为 true
                    if (type == 2) {
                        columnsNode.addAttribute(buildAttributeNode("header", "true"));
                    }

                    debugStr.append("</columns>");
                    logger.debug("@@@ 输出...\n{}", debugStr.toString());
                    return columnsNode;
                }
            };
        }


        /**
         * 增加 column 节点属性 - 依据数据库列元数据
         *
         * @param node
         * @param attributes
         * @param debugStr
         */
        private static void addColumnNodeAttributes(ConfigurationNode node, Map<String, Object> attributes, StringBuffer debugStr) {
            Integer dataType = (Integer) attributes.get("DATA_TYPE");
            Integer columnSize = (Integer) attributes.get("COLUMN_SIZE");
            Integer decimalDigits = (Integer) attributes.get("DECIMAL_DIGITS");

            switch (dataType) {
                case Types.BIGINT:
                case Types.INTEGER:
                case Types.TINYINT:
                case Types.SMALLINT:
                    node.addAttribute(buildAttributeNode("type", "format"));
                    node.addAttribute(buildAttributeNode("value", "int"));
                    debugStr.append("\t<column type=\"format\" value=\"int\">\n");
                    break;
                case Types.DECIMAL:
                case Types.DOUBLE:
                case Types.FLOAT:
                case Types.NUMERIC:
                case Types.REAL:
                    node.addAttribute(buildAttributeNode("type", "format"));
                    node.addAttribute(buildAttributeNode("value", "number-" + columnSize + "-" + decimalDigits));
                    debugStr.append("\t<column type=\"format\" value=\"number-" + columnSize + "-" + decimalDigits + "\">\n");
                    break;
                case Types.DATE:
                case Types.TIME:
                case Types.TIMESTAMP:
                    node.addAttribute(buildAttributeNode("type", "format"));
                    node.addAttribute(buildAttributeNode("value", "date"));
                    debugStr.append("\t<column type=\"format\" value=\"date\">\n");
                    break;
                default:
                    debugStr.append("\t<column>\n");
                    break;
            }
        }

    }

}
