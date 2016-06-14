package zw.wormsleep.tools.etl.database;

import org.apache.commons.beanutils.DynaBean;
import org.apache.commons.beanutils.ResultSetDynaClass;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zw.wormsleep.tools.etl.multitask.MultiTaskMultiThread;
import zw.wormsleep.tools.etl.multitask.Task;
import zw.wormsleep.tools.etl.multitask.multithread.NTaskPerThread;
import zw.wormsleep.tools.etl.multitask.task.Table2TableTask;
import zw.wormsleep.tools.etl.utils.ConfigBuilderUtils;
import zw.wormsleep.tools.etl.utils.ConfigParserUtils;

import java.beans.PropertyVetoException;
import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.*;

public class DatabaseHelper {
    static final Logger logger = LoggerFactory.getLogger(DatabaseHelper.class);

    static final String COMMA = ",";
    static final String CONST_AND = " and ";

    /**
     * 描述数据库元数据中列定义信息
     *
     * @author zhaowei
     */
    public static class SQLTypeMetaData {
        private static Map<String, Integer> types;

        private SQLTypeMetaData() {
        }

        private static void initTypes() {
            types = new HashMap<String, Integer>();
            // 元数据中定义列
            types.put("TABLE_CAT", 1);
            types.put("TABLE_SCHEM", 2);
            types.put("TABLE_NAME", 3);
            types.put("COLUMN_NAME", 4);
            types.put("DATA_TYPE", 5);
            types.put("TYPE_NAME", 6);
            types.put("COLUMN_SIZE", 7);
            types.put("DECIMAL_DIGITS", 9);
            types.put("NULLABLE", 11);
            types.put("COLUMN_DEF", 13);
        }

        /**
         * 获取元数据定义列名称和索引
         *
         * @return
         */
        public static Map<String, Integer> getDefinition() {
            if (types == null) {
                initTypes();
            }
            return types;
        }
    }

    /**
     * 将传入的值按若小于零则为零处理 例如: value="field1,field2" table="tablename"
     * additionalWhere=" whereexpress " 结果: update tablename set field1=case
     * when field1<0 then 0 else field1 end,field1=case when field2<0 then 0
     * else field1 end where whereexpress
     *
     * @param value
     * @param table
     * @param additionalWhere
     * @return
     */
    public static String getIfValueLessThanZeroThenSetZero(String value,
                                                           String table, String additionalWhere) {
        StringBuffer express = new StringBuffer();
        String[] fields = value.split(",");
        int fieldCount = fields.length;

        express.append(" update " + table + " set ");
        for (int i = 0; i < fieldCount; i++) {
            express.append(fields[i] + "=" + "case when " + fields[i]
                    + "<0 then 0 else " + fields[i] + " end,");
        }

        String exp = express.toString();

        if (additionalWhere != null && !additionalWhere.equals("")) {
            return exp.substring(0, exp.length() - 1) + " where "
                    + additionalWhere;
        } else {
            return exp.substring(0, exp.length() - 1);
        }

    }

    /**
     * 将转入的按逗号分隔的字段名增加 NULL 值处理生成表达式 例如: value = "field1,field2,field3" table =
     * "tablename" 返回: update tablename set
     * field1=isnull(field1,0),field2=isnull(field2,0),field3=isnull(field3,0)
     *
     * @param value 逗号分隔字段名
     * @param table 表名
     * @return
     */
    public static String getIsnullFieldsExpress(String value, String table) {
        StringBuffer express = new StringBuffer();
        String[] fields = value.split(",");
        int fieldCount = fields.length;

        express.append(" update " + table + " set ");
        for (int i = 0; i < fieldCount; i++) {
            express.append(fields[i] + "=" + "isnull(" + fields[i] + ",0),");
        }

        String exp = express.toString();

        return exp.substring(0, exp.length() - 1);
    }

    /**
     * 替换 “：parameterName”内容
     *
     * @param sql
     * @param parameters
     * @return
     */
    public static String getReplacedSQL(String sql,
                                        Map<String, String> parameters) {
        String replacedSQL = sql;

        if (parameters != null && parameters.size() > 0) {
            for (String key : parameters.keySet()) {
                replacedSQL = replacedSQL.replace(":" + key,
                        parameters.get(key));
            }
        }

        return replacedSQL;
    }

    /**
     * 动态结果集 注意：好像字段都要求小写（不论 SQL 和存储过程）
     *
     * @param sql  标准 SQL 语句（SQL DDL EXEC）
     * @param conn
     * @return
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static List<Map<String, Object>> getResultSetDynaData(String sql,
                                                                 Connection conn) {
        List resultSetList = new ArrayList();

        Statement stmt = null;
        ResultSet rs = null;

        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
            // 获取列名集合
            List<String> columnName = new ArrayList<String>();
            int columnCount = rs.getMetaData().getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                columnName.add(rs.getMetaData().getColumnName(i));
                logger.debug("@@@ 【动态】列名：{}", rs.getMetaData().getColumnName(i));
            }
            // 获取结果集并按列名填充
            Map<String, Object> rsMap = null;
            Iterator rows = (new ResultSetDynaClass(rs)).iterator();
            while (rows.hasNext()) {
                DynaBean row = (DynaBean) rows.next();
                rsMap = new HashMap<String, Object>();
                for (String cn : columnName) {
                    rsMap.put(cn, row.get(cn));
                }
                resultSetList.add(rsMap);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    logger.error("SQL 异常 !", e);
                }
            }

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    logger.error("SQL 异常 !", e);
                }
            }
        }
        return resultSetList;
    }

    /**
     * 动态结果集 注意：好像字段都要求小写（不论 SQL 和存储过程）
     *
     * @param sql      标准 SQL 语句（SQL DDL EXEC）
     * @param database 注册数据库名称
     * @return
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static List<Map<String, Object>> getResultSetDynaData(String sql,
                                                                 String database) {
        List resultSetList = new ArrayList();

        Connection conn = null;

        try {

            conn = ConnectionPool.getConnection(database,
                    ConfigParserUtils.getDatabaseConfiguration(database));
            resultSetList = getResultSetDynaData(sql, conn);

        } catch (ConfigurationException e) {
            logger.error("配置异常 !", e);
        } catch (PropertyVetoException e) {
            logger.error("属性异常 !", e);
        } catch (SQLException e) {
            logger.error("SQL 异常 !", e);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                    conn = null;
                } catch (SQLException e) {
                    logger.error("SQL 异常 !", e);
                }
            }
        }

        return resultSetList;
    }

    /**
     * 查询结果集
     *
     * @param sql  标准 SQL 语句（SQL DDL EXEC）
     * @param conn
     * @return
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static List<Map<String, Object>> executeQuery(String sql,
                                                         Connection conn) {
        logger.info("\nSQL:\n{}", sql);
        List resultSetList = new ArrayList();

        Statement stmt = null;
        ResultSet rs = null;

        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
            // 获取列名集合
            List<String> cols = new ArrayList<String>();
            int columnCount = rs.getMetaData().getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                String columnName = rs.getMetaData().getColumnLabel(i);
                cols.add(columnName);
                logger.debug("@@@ 列名：{}", columnName);
            }
            // 获取结果集并按列名填充
            Map<String, Object> rsMap = null;
            while (rs.next()) {
                rsMap = new HashMap<String, Object>();
                for (int i = 0; i < columnCount; i++) {
                    rsMap.put(cols.get(i), rs.getObject(i + 1));
                }
                resultSetList.add(rsMap);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    logger.error("SQL 异常 !", e);
                }
            }

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    logger.error("SQL 异常 !", e);
                }
            }
        }
        return resultSetList;
    }

    /**
     * 动态结果集 注意：好像字段都要求小写（不论 SQL 和存储过程）
     *
     * @param sql      标准 SQL 语句（SQL DDL EXEC）
     * @param database 注册数据库名称
     * @return
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static List<Map<String, Object>> executeQuery(String sql,
                                                         String database) {
        List resultSetList = new ArrayList();

        Connection conn = null;

        try {

            conn = ConnectionPool.getConnection(database,
                    ConfigParserUtils.getDatabaseConfiguration(database));
            resultSetList = executeQuery(sql, conn);

        } catch (ConfigurationException e) {
            logger.error("配置异常 !", e);
        } catch (PropertyVetoException e) {
            logger.error("属性异常 !", e);
        } catch (SQLException e) {
            logger.error("SQL 异常 !", e);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                    conn = null;
                } catch (SQLException e) {
                    logger.error("SQL 异常 !", e);
                }
            }
        }

        return resultSetList;
    }


    /**
     * 执行更新 SQL
     *
     * @param sql      标准 SQL 语句（SQL DDL EXEC）
     * @param database 注册数据库名称
     * @return
     */
    public static int executeUpdate(String sql, String database) {
        int result = 0;
        Connection conn = null;

        try {

            conn = ConnectionPool.getConnection(database,
                    ConfigParserUtils.getDatabaseConfiguration(database));
            result = executeUpdate(conn, sql);

        } catch (ConfigurationException e) {
            logger.error("配置异常 !", e);
        } catch (PropertyVetoException e) {
            logger.error("属性异常 !", e);
        } catch (SQLException e) {
            logger.error("SQL 异常 !", e);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                    conn = null;
                } catch (SQLException e) {
                    logger.error("SQL 异常 !", e);
                }
            }
        }

        return result;
    }

    /**
     * 执行更新 SQL
     *
     * @param conn
     * @param sql  标准 SQL 语句（SQL DDL EXEC）
     * @return
     */
    public static int executeUpdate(Connection conn, String sql) {
        int result = 0;
        Statement stat = null;

        try {
            stat = conn.createStatement();
            logger.debug("@@@ Execute Update - SQL\nSQL: \n{}", sql);
            result = stat.executeUpdate(sql);
        } catch (SQLException e) {
            logger.error("SQL 异常 !", e);
        } finally {
            if (stat != null) {
                try {
                    stat.close();
                    stat = null;
                } catch (SQLException e) {
                    logger.error("SQL 异常 !", e);
                }
            }
        }

        return result;
    }

    /**
     * 执行批量更新 SQL
     *
     * @param sqls
     * @param database
     * @return
     */
    public static int[] executeUpdate(List<String> sqls, String database) {
        int result[] = null;

        Connection conn = null;

        try {

            conn = ConnectionPool.getConnection(database,
                    ConfigParserUtils.getDatabaseConfiguration(database));
            result = executeUpdate(conn, sqls);

        } catch (ConfigurationException e) {
            logger.error("配置异常 !", e);
        } catch (PropertyVetoException e) {
            logger.error("属性异常 !", e);
        } catch (SQLException e) {
            logger.error("SQL 异常 !", e);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                    conn = null;
                } catch (SQLException e) {
                    logger.error("SQL 异常 !", e);
                }
            }
        }

        return result;
    }

    /**
     * 执行批量更新 SQL
     *
     * @param conn
     * @param sqls
     * @return
     */
    public static int[] executeUpdate(Connection conn, List<String> sqls) {
        int result[] = null;
        Statement stat = null;

        try {
            stat = conn.createStatement();

            for (String sql : sqls) {
                stat.addBatch(sql);
                logger.debug("@@@ Execute Update - SQL\nSQL: \n{}", sql);
            }

            result = stat.executeBatch();
        } catch (SQLException e) {
            logger.error("SQL 异常 !", e);
        } finally {
            if (stat != null) {
                try {
                    stat.close();
                    stat = null;
                } catch (SQLException e) {
                    logger.error("SQL 异常 !", e);
                }
            }
        }

        return result;
    }

    /**
     * 获取批量插入语句（根据数据库类型）
     *
     * @param dbType 数据库类型 ( sybase oracle mysql ... ) 若为 NULL 值则按默认处理
     * @param table  表名
     * @param fields 字段 ( 主键值 true )
     * @return
     */
    public static String getBatchInsertSQL(String dbType, String table,
                                           Map<String, Boolean> fields,
                                           Map<String, Boolean> updateFields,
                                           boolean ignoreUpdate) {
        String batchInsertSQL = null;

        if (dbType != null && !dbType.equals("")) {
            if (dbType.equalsIgnoreCase("sybase")) {
                batchInsertSQL = assembleSybaseBatchInsertSQL(table, fields, updateFields, ignoreUpdate);
            } else if (dbType.equalsIgnoreCase("mysql")) {
                batchInsertSQL = assembleMySQLBatchInsertSQL(table, fields);
            } else if (dbType.equalsIgnoreCase("oracle")) {
                batchInsertSQL = assembleOracleBatchInsertSQL(table, fields);
            } else {
                batchInsertSQL = assembleDefaultBatchInsertSQL(table, fields);
            }
        } else {
            batchInsertSQL = assembleDefaultBatchInsertSQL(table, fields);
        }

        return batchInsertSQL;
    }

    /**
     * 通过预定义字段获取 Select SQL 供后期获取这些字段对应的数据库字段类型
     *
     * @param table  表名
     * @param fields 字段 ( 主键值 true )
     * @return
     */
    public static String getSelectSQL(String table, Map<String, Boolean> fields) {
        StringBuffer sql = new StringBuffer();
        StringBuffer fieldsPart = new StringBuffer();
        sql.append("select ");
        for (String field : fields.keySet()) {
            fieldsPart.append(field + ",");
        }
        sql.append(fieldsPart.substring(0, fieldsPart.length() - 1));
        sql.append(" from ");
        sql.append(table);
        sql.append(" where 1=0 ");
        return sql.toString();
    }

    /**
     * MySQL 数据库排除重复插入语句
     *
     * @param table  表名
     * @param fields 字段 ( 主键值 true )
     * @return
     */
    public static String assembleMySQLBatchInsertSQL(String table,
                                                     Map<String, Boolean> fields) {
        StringBuffer sql = new StringBuffer();
        StringBuffer fieldsPart = new StringBuffer();
        StringBuffer valuesPart = new StringBuffer();
        StringBuffer updatePart = new StringBuffer();

        // 动态生成部分
        for (String key : fields.keySet()) {
            fieldsPart.append(key + COMMA);
            valuesPart.append(":" + key + COMMA);
            if (fields.get(key)) {
                updatePart.append(key + "=:" + key + COMMA);
            }
        }
        // 静态组装部分
        sql.append("insert into " + table);
        sql.append(" (" + fieldsPart.substring(0, fieldsPart.length() - 1)
                + ")");
        sql.append(" values ("
                + valuesPart.substring(0, valuesPart.length() - 1) + ")");
        if (updatePart.length() > 1) {
            sql.append(" on duplicate key update ");
            sql.append(updatePart.substring(0, updatePart.length() - 1));
        }
        // update part
        return sql.toString();
    }

    /**
     * Sybase 数据库排除重复插入语句
     *
     * @param table  表名
     * @param fields 字段 ( 主键值 true )
     * @return IF EXISTS ( SELECT 1 FROM TABLE_NAME WHERE KEY1=:KEY1 AND KEY2=:KEY2 )
     * UPDATE TABLE_NAME SET FIELD1=:FIELD1,FIELD2=:FIELD2,...
     * WHERE KEY=:KEY1 AND KEY2=KEY2
     * ELSE
     * INSERT INTO TABLE_NAME ( FIELD1,FIELD2,FIELD3,... ) VALUES ( :FIELD1,:FIELD2,:FIELD3 )
     */
    public static String assembleSybaseBatchInsertSQL(String table,
                                                      Map<String, Boolean> fields,
                                                      Map<String, Boolean> updateFields,
                                                      boolean ignoreUpdate) {
        StringBuffer sql = new StringBuffer();
        StringBuffer fieldsPart = new StringBuffer();
        StringBuffer valuesPart = new StringBuffer();
        StringBuffer updatePart = new StringBuffer();
        StringBuffer wherePart = new StringBuffer();

        // 动态生成部分
        for (String key : fields.keySet()) {

            fieldsPart.append(key + COMMA);
            valuesPart.append(":" + key + COMMA);
            if (fields.get(key)) {
                wherePart.append(key + "=:" + key + CONST_AND);
            }
            if (updateFields.get(key)) {
                updatePart.append(key + "=:" + key + COMMA);
            }
        }
        // 静态组装部分
        if (wherePart.length() > 0) { // 若需要排重语句
            sql.append("if exists (select 1 from " + table);
            sql.append(" where "
                    + wherePart.substring(0, wherePart.length() - 5) + ")");
            sql.append(" update " + table + " set "
                    + updatePart.substring(0, updatePart.length() - 1));
            // 通过特殊语句支持忽略 update 语句效果
            if (ignoreUpdate) {
                sql.append(" where 1=0 ");
            } else {
                sql.append(" where "
                        + wherePart.substring(0, wherePart.length() - 5));
            }
            sql.append(" else ");
        }
        sql.append(" insert into " + table);
        sql.append(" (" + fieldsPart.substring(0, fieldsPart.length() - 1)
                + ")");
        sql.append(" values ("
                + valuesPart.substring(0, valuesPart.length() - 1) + ")");
        // update part
        return sql.toString();
    }

    /**
     * Oracle 数据库排除重复插入语句
     *
     * @param table  表名
     * @param fields 字段 ( 主键值 true )
     * @return MERGE INTO TABLE_NAME M
     * USING (SELECT :C1 AS C1, :C2 AS C2 FROM DUAL) N
     * ON (M.C1=N.C1)
     * WHEN MATCHED THEN
     * UPDATE SET M.C2=N.C2
     * WHEN NOT MATCHED THEN
     * INSERT (C1,C2) VALUES(N.C1,N.C2)
     */
    public static String assembleOracleBatchInsertSQL(String table,
                                                      Map<String, Boolean> fields) {
        StringBuffer sql = new StringBuffer();
        StringBuffer fieldsPart = new StringBuffer();
        StringBuffer valuesPart = new StringBuffer();
        StringBuffer updatePart = new StringBuffer();
        StringBuffer wherePart = new StringBuffer();
        StringBuffer asfieldPart = new StringBuffer();

        // 动态生成部分
        for (String key : fields.keySet()) {

            fieldsPart.append(key + COMMA);
            valuesPart.append(":" + key + COMMA);
            asfieldPart.append(":" + key + " AS " + key + COMMA);
            if (fields.get(key)) {
                wherePart.append("M." + key + "=N." + key + CONST_AND);
            }
            updatePart.append("M." + key + "=N." + key + COMMA);
        }
        // 静态组装部分
        if (wherePart.length() > 0) { // 若需要排重语句
            sql.append(" MERGE INTO " + table + " M ");
            sql.append(" USING ( SELECT "
                    + asfieldPart.substring(0, asfieldPart.length() - 1));
            sql.append(" FROM DUAL) N ");
            sql.append(" ON (" + wherePart.substring(0, wherePart.length() - 5) + ")");
            sql.append(" WHEN MATCHED THEN ");
            sql.append(" UPDATE SET "
                    + updatePart.substring(0, updatePart.length() - 1));
            sql.append(" WHEN NOT MATCHED THEN ");
        }
        sql.append(" insert into " + table);
        sql.append(" (" + fieldsPart.substring(0, fieldsPart.length() - 1)
                + ")");
        sql.append(" values ("
                + valuesPart.substring(0, valuesPart.length() - 1) + ")");
        // update part
        return sql.toString();
    }

    /**
     * 默认数据库批量插入语句 - 非排重
     *
     * @param table  表名
     * @param fields 字段 ( 主键值 true )
     * @return
     */
    public static String assembleDefaultBatchInsertSQL(String table,
                                                       Map<String, Boolean> fields) {
        StringBuffer sql = new StringBuffer();
        StringBuffer fieldsPart = new StringBuffer();
        StringBuffer valuesPart = new StringBuffer();

        // 动态生成部分
        for (String key : fields.keySet()) {
            fieldsPart.append(key + COMMA);
            valuesPart.append(":" + key + COMMA);
        }
        sql.append(" insert into " + table);
        sql.append(" (" + fieldsPart.substring(0, fieldsPart.length() - 1)
                + ")");
        sql.append(" values ("
                + valuesPart.substring(0, valuesPart.length() - 1) + ")");
        // update part
        return sql.toString();
    }

    /**
     * PrepareStatement 参数填充
     *
     * @param pstmt  待填充 PrepareStatement
     * @param data   参数数据集合
     * @param params 参数位置集合
     * @param types  参数数据库字段类型集合
     */
    public static void fillParamters(PreparedStatement pstmt,
                                     Map<String, Object> data, Map<Integer, String> params,
                                     Map<String, Integer> types) {
        // logger.debug("@@@ 参数 SIZE [data, params] -> [{}, {}]", data.size(),
        // params.size());
        String name = null;
        Object value = null;
        Integer type = null;
        for (Integer index : params.keySet()) {
            name = params.get(index);
            value = data.get(name);
            type = types.get(name);
            try {
                pstmt.setObject(index, value, type, 6);
            } catch (SQLException e) {
                logger.error("SQL 异常 !", e);
            }
        }
    }


    /**
     * 打印数据库表元数据对象
     *
     * @param database
     * @param printer
     */
    public static void printDatabaseMetadata(String database, DatabaseMetadataPrinter printer) {
        Connection conn = null;
        DatabaseMetaData dmd = null;

        try {
            // 获取配置文件中的数据连接参数集并创建连接池
            conn = ConnectionPool.getConnection(database,
                    ConfigParserUtils.getDatabaseConfiguration(database));
            // 获取数据库元数据对象
            dmd = conn.getMetaData();

            // 调用打印接口方法
            printer.print(conn, dmd);

        } catch (ConfigurationException e) {
            logger.error("配置异常 !", e);
        } catch (PropertyVetoException e) {
            logger.error("属性异常 !", e);
        } catch (SQLException e) {
            logger.error("SQL 异常 !", e);
        } finally {
            if (conn != null) {
                try {
                    conn.setAutoCommit(true);
                    conn.close();
                    conn = null;
                } catch (SQLException e) {
                    logger.error("SQL 异常 !", e);
                }
            }
        }
    }

    /**
     * 异构或同构数据库表对表拷贝
     *
     * @param srcDatabase         源数据库
     * @param srcCatalog          catalog ( null 为全部 )
     * @param srcSchemaPattern    schema 模式 ( null 为全部 其中对于 Oracle 数据库最好指明 )
     * @param srcTableNamePattern 表名模式
     * @param destDatabase        目标数据库
     * @param destCatalog         catalog ( null 为全部 )
     * @param destSchemaPattern   schema 模式 ( null 为全部 其中对于 Oracle 数据库最好指明 )
     * @param dropandcreate       是否重建表
     * @param copydata            是否拷贝数据
     * @param threadCount         线程数
     */
    @SuppressWarnings("unchecked")
    public static void tables2tables(String srcDatabase, String srcCatalog, String srcSchemaPattern, String srcTableNamePattern, String destDatabase, String destCatalog, String destSchemaPattern, boolean dropandcreate, boolean copydata, int threadCount) {
        logger.info("@@@ 数据库表对表批量拷贝 \n 初始化参数 \n 1. 源数据库参数 - [配置节点]: {} [CATALOG]: {} [SCHEMAPATTERN]: {} [TABLENAMEPATTERN]: {} \n 2. 目标数据库参数 - [配置节点]: {} [CATALOG]: {} [SCHEMAPATTERN]: {} \n 3. 执行参数 - [自动建表]: {} [传输表数据]: {} [预分配线程数]: {}", srcDatabase, srcCatalog, srcSchemaPattern, srcTableNamePattern, destDatabase, destCatalog, destSchemaPattern, dropandcreate, copydata, threadCount);
        Map<String, Map<String, Map<String, Object>>> srcTablesObject = null;
        Map<String, String> tablesDDL = new LinkedHashMap<String, String>();
        List<String> srcTables = new ArrayList<String>();
        List<String> destTables = new ArrayList<String>();

        String srcDatabaseType, destDatabaseType;

        Connection srcConn = null;
        Connection destConn = null;
        ResultSet rs = null;
        try {
            // @@@ 建表语句生成部分 ...
            logger.info("@@@ 分析数据库元数据信息 ...");
            // 获取数据库 - 连接对象
            srcConn = ConnectionPool.getConnection(srcDatabase, ConfigParserUtils.getDatabaseConfiguration(srcDatabase));
            destConn = ConnectionPool.getConnection(destDatabase, ConfigParserUtils.getDatabaseConfiguration(destDatabase));
            // 元数据对象
            DatabaseMetaData srcDatabaseMetaData = srcConn.getMetaData();
            DatabaseMetaData destDatabaseMetaData = destConn.getMetaData();
            // 数据库类型
            srcDatabaseType = getDatabaseType(srcDatabaseMetaData.getDatabaseProductName());
            destDatabaseType = getDatabaseType(destDatabaseMetaData.getDatabaseProductName());
            logger.info("@@@ 数据库产品 - 源数据库: {} 目标数据库: {} ", srcDatabaseType, destDatabaseType);
            // 获取源数据库 - 表元数据
            logger.info("@@@ 分析源数据库表信息 ...");
            rs = srcDatabaseMetaData.getTables(srcCatalog, srcSchemaPattern, srcTableNamePattern, new String[]{"TABLE"});
            srcTables = getTablesList(rs);
            // 分析并生成 - 源数据库 - 表元数据描述对象集 Map<String, Map<String, Object>>
            rs = srcDatabaseMetaData.getColumns(srcCatalog, srcSchemaPattern, srcTableNamePattern, null);
            srcTablesObject = getTablesMetadata(rs, srcTables);
            logger.info("@@@ 源数据库表分析完毕 ! 待处理表总数: {}", srcTablesObject.size());
            // 分析并生成 - 目标数据库 - 建表语句
            // *** 数据字段类型转换器
            TypeConverter typeConverter = (TypeConverter) Class.forName("zw.wormsleep.tools.etl.database.converter." + StringUtils.capitalize(srcDatabaseType) + "2" + StringUtils.capitalize(destDatabaseType) + "TypeConverter").newInstance();
            // *** 数据库 DDL 生成器
            DDLMaker ddlMaker = (DDLMaker) Class.forName("zw.wormsleep.tools.etl.database.maker." + StringUtils.capitalize(destDatabaseType) + "DDLMaker").newInstance();
            for (String tableName : srcTablesObject.keySet()) {
                // 获取源数据库 - 主键元数据
                logger.debug("@@@ 分析表 {} 主键信息 ...", tableName);
                rs = srcDatabaseMetaData.getPrimaryKeys(null, null, tableName);
                // 分析并生成 - 目标数据库 - 主键元数据描述对象集 Map<String, Object> {{key: name, value: keyname}, {key: keys, value: [key1,key2,key3]}}
                Map<String, Object> keys = getPrimarykeysMetadata(rs);
                // 生成目标数据库 - 主键 DDL SQL
                String primarykeyName = (String) keys.get("name");
                List<String> primarykeys = (List<String>) keys.get("keys");
                // 获取单表
                Map<String, Map<String, Object>> columnsMetadata = srcTablesObject.get(tableName);
                // 生成目标数据库 - 建表 DDL SQL
                String tableDDL = ddlMaker.createTable(tableName, columnsMetadata, typeConverter, primarykeyName, primarykeys);
                tablesDDL.put(tableName, tableDDL);
                logger.debug("@@@ {} 表建表语句 \n {}", tableName, tableDDL);
            }

            // @@@ 目标数据库建表部分
            // 目标数据库 - 现有表 - 由于是目标数据库因此仅查询类型为表的元数据
            logger.info("@@@ 分析目标数据库表信息 ...");
            rs = destDatabaseMetaData.getTables(destCatalog, destSchemaPattern, "%", new String[]{"TABLE"});
            destTables = getTablesList(rs);
            logger.info("@@@ 目标数据库表分析完毕 !");
            // 目标数据库 - 建表
            if (dropandcreate) {
                logger.info("@@@ 准备删除和创建目标表 ... ");
                for (String tableName : tablesDDL.keySet()) {
                    if (destTables.contains(tableName)) {
                        DatabaseHelper.executeUpdate(destConn, getDropTableSQL(destDatabaseType, tableName));
                        logger.info("@@@ 删除表 {} ", tableName);
                    }
                    DatabaseHelper.executeUpdate(destConn, tablesDDL.get(tableName));
                    logger.info("@@@ 创建表 {} ", tableName);
                }
            }

            // @@@ 动态生成配置文件
            File configuration = new File("etl-" + srcDatabase + "-" + destDatabase + "-config.xml");
            ConfigBuilderUtils.createTables2TablesConfiguration(configuration, srcDatabase, srcDatabaseType, destDatabase, destDatabaseType, srcTables);
            // @@@ 执行数据拷贝 - 支持多线程
            if (copydata) {
                logger.info("@@@ 准备表对表数据传输 ... ");
                // 多任务初始化
                List<Task> tasks = new ArrayList<Task>();
                for (String businessType : srcTables) {
                    tasks.add(new Table2TableTask(businessType, configuration));
                }
                logger.info("@@@ 待传输表总数: {} 分配线程数: {}", tasks.size(), threadCount);
                // 多线程执行
                MultiTaskMultiThread mtmt = new NTaskPerThread(tasks, threadCount);
                mtmt.executeBatch();
            }


        } catch (ConfigurationException e) {
            logger.error("配置异常 !", e);
        } catch (PropertyVetoException e) {
            logger.error("属性异常 !", e);
        } catch (SQLException e) {
            logger.error("SQL 异常 !", e);
        } catch (InstantiationException e) {
            logger.error("实例化异常 !", e);
        } catch (IllegalAccessException e) {
            logger.error("非法访问异常 !", e);
        } catch (ClassNotFoundException e) {
            logger.error("类未找到异常 !", e);
        } finally {
            if (srcConn != null) {
                try {
                    srcConn.setAutoCommit(true);
                    srcConn.close();
                    srcConn = null;
                } catch (SQLException e) {
                    logger.error("SQL 异常 !", e);
                }
            }

            if (destConn != null) {
                try {
                    destConn.setAutoCommit(true);
                    destConn.close();
                    destConn = null;
                } catch (SQLException e) {
                    logger.error("SQL 异常 !", e);
                }
            }
        }
    }


    /**
     * 获取表元数据描述对象集
     *
     * @param rs     从 DatabaseMetaData 对象方法 getColumns(...) 中获取的元数据结果集
     * @param tables 通过 DatabaseMetaData 对象方法 getTables(..., new String[]{"TABLE"}) 中获取的仅表对象列表
     * @return 结果集结构描述如下
     * {
     * {
     * key: table1,
     * value: {
     * {
     * key: column1,
     * vlaue: {
     * {key: COLUMN_NAME, value: xxx},
     * {key: DATA_TYPE, value: xxx},
     * ...
     * }
     * }
     * }
     * },
     * ...
     * }
     */
    private static Map<String, Map<String, Map<String, Object>>> getTablesMetadata(ResultSet rs, List<String> tables) {
        Map<String, Map<String, Map<String, Object>>> result = new LinkedHashMap<String, Map<String, Map<String, Object>>>();
        Object value = null;
        // 依表名列表组装排序的待比对表名数组
        String[] ts = new String[tables.size()];
        ts = tables.toArray(ts);
        Arrays.sort(ts);
        try {
            while (rs.next()) {
                // 获取单行记录中的表名
                String tableName = rs.getString(3);
                //* 依表名列表精确判断是否为表
                if (Arrays.binarySearch(ts, tableName) < 0) continue;
                // 依据该表名为主键判断是否已创建, 若无则新建对象
                Map<String, Map<String, Object>> t = result.containsKey(tableName) ? result.get(tableName) : new LinkedHashMap<String, Map<String, Object>>();
                // 获取单行记录中的列名
                String columnName = rs.getString(4);
                // 依据该列名新建列元数据描述对象集
                Map<String, Object> c = new HashMap<String, Object>();
                // 依据元数据对象要素获取内容
                for (String key : SQLTypeMetaData.getDefinition().keySet()) {
                    value = rs.getObject(SQLTypeMetaData.getDefinition().get(key));
                    c.put(key, value);
//					logger.debug("@@@ [key, value] {}, {}", key, value);
                }
                // 将列元数据对象集推入表对象集
                t.put(columnName, c);
                result.put(tableName, t);
            }
        } catch (SQLException e) {
            logger.error("SQL 异常 !", e);
        }

        return result;
    }

    /**
     * 获取表列元数据对象集
     *
     * @param database  数据库节点名称
     * @param catalog   目录
     * @param tableName 表名
     * @return
     */
    public static Map<String, Map<String, Object>> getColumnsMetadata(String database, String catalog, String tableName) {

        Map<String, Map<String, Object>> columns = new LinkedHashMap<String, Map<String, Object>>();

        Connection conn = null;
        ResultSet rs = null;
        try {
            // 获取数据库 - 连接对象
            conn = ConnectionPool.getConnection(database, ConfigParserUtils.getDatabaseConfiguration(database));
            // 元数据对象
            DatabaseMetaData srcDatabaseMetaData = conn.getMetaData();
            // 获取源数据库 - 表元数据
            rs = srcDatabaseMetaData.getColumns(catalog, null, tableName, null);
            // 组装列元数据集对象
            Object value = null;
            String tname = null;
            String columnName = null;
            Map<String, Object> cs = null;
            while (rs.next()) {
                // 获取单行记录中的表名
                tname = rs.getString(3);
                if (tableName.equals(tname)) {
                    // 获取单行记录中的列名
                    columnName = rs.getString(4);
                    // 依据该列名新建列元数据描述对象集
                    cs = new HashMap<String, Object>();
                    // 依据元数据对象要素获取内容
                    for (String key : SQLTypeMetaData.getDefinition().keySet()) {
                        value = rs.getObject(SQLTypeMetaData.getDefinition().get(key));
                        cs.put(key, value);
//					logger.debug("@@@ [key, value] {}, {}", key, value);
                    }
                    // 将列元数据对象集推入表对象集
                    columns.put(columnName, cs);
                }
            }
        } catch (ConfigurationException e) {
            logger.error("配置异常 !", e);
        } catch (PropertyVetoException e) {
            logger.error("属性异常 !", e);
        } catch (SQLException e) {
            logger.error("SQL 异常 !", e);
        }


        return columns;
    }

    /**
     * 获取主键元数据描述信息对象
     *
     * @param rs
     * @return
     */
    private static Map<String, Object> getPrimarykeysMetadata(ResultSet rs) {
        Map<String, Object> result = new HashMap<String, Object>();
        List<String> keys = new ArrayList<String>();
        try {
            while (rs.next()) {
                result.put("name", rs.getObject(6));
                keys.add(rs.getString(4));
            }
            result.put("keys", keys);
        } catch (SQLException e) {
            logger.error("SQL 异常 !", e);
        }

        return result;
    }

    /**
     * 根据产品名称判断数据库类型
     *
     * @param productName
     * @return
     */
    public static String getDatabaseType(String productName) {
        String type = "";
        String sybase = "Adaptive Server Enterprise";
        String oracle = "Oracle";

        if (productName.contains(sybase)) {
            type = "sybase";
        } else if (productName.contains(oracle)) {
            type = "oracle";
        }

        return type;
    }

    /**
     * 根据数据库类型生成对应的 drop table 语句
     *
     * @param databaseType
     * @param tableName
     * @return
     */
    public static String getDropTableSQL(String databaseType, String tableName) {

//		if(databaseType.equals("sybase")) {
//			sql = "if exists( select 1 from sysobjects where name='"+tableName+"' and sysstat & 15 = 3) \n drop table " + tableName;
//		} else {
//			sql = "drop table " + tableName;
//		}

        return "drop table " + tableName;
    }

    /**
     * 根据表元数据信息生成存在表的列表
     *
     * @param rs 表元数据
     * @return
     */
    public static List<String> getTablesList(ResultSet rs) {
        List<String> result = new ArrayList<String>();
        try {
            while (rs.next()) {
                result.add(rs.getString(3));
            }
        } catch (SQLException e) {
            logger.error("SQL 异常 !", e);
        }

        return result;
    }

    /**
     * 扫描数据库表元信息，并返回扫描结果列表
     * @param database 数据库配置节点名
     * @param catalog 目录
     * @param schemaPattern 大纲匹配模式
     * @param tableNamePattern 表名匹配模式
     * @param types 表类型 （默认为 "TABLE"） table type. Typical types are "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
     * @return
     */
    public static List<String> getTablesList(String database, String catalog, String schemaPattern, String tableNamePattern, String[] types) {

        List<String> tables = new ArrayList<String>();

        Connection conn = null;
        ResultSet rs = null;
        try {
            // 获取数据库 - 连接对象
            conn = ConnectionPool.getConnection(database, ConfigParserUtils.getDatabaseConfiguration(database));
            // 元数据对象
            DatabaseMetaData srcDatabaseMetaData = conn.getMetaData();
            // 获取源数据库 - 表元数据
            rs = srcDatabaseMetaData.getTables(
                    (catalog != null && !catalog.trim().equals("")) ? catalog : null,
                    (schemaPattern != null && !schemaPattern.trim().equals("")) ? schemaPattern : null,
                    (tableNamePattern != null && !tableNamePattern.trim().equals("")) ? tableNamePattern : "%",
                    (tableNamePattern != null) ? types : new String[]{"TABLE"}
            );
            while (rs.next()) {
                tables.add(rs.getString(3));
            }
        } catch (ConfigurationException e) {
            logger.error("配置异常 !", e);
        } catch (PropertyVetoException e) {
            logger.error("属性异常 !", e);
        } catch (SQLException e) {
            logger.error("SQL 异常 !", e);
        }

        return tables;

    }


    /**
     * 获取表字段名集合
     *
     * @param database  数据库配置节点
     * @param tableName 表名
     * @return
     */
    public static List<String> getTableColumnsName(String database, String tableName) {
        List<String> columns = new ArrayList<String>();

        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;

        try {
            conn = ConnectionPool.getConnection(database, ConfigParserUtils.getDatabaseConfiguration(database));
            stmt = conn.createStatement();
            // 由于不需要对结果集内容进行处理, 而仅对结果集的元数据进行处理, 因此设定该值为 1
            stmt.setFetchSize(1);
            String sql = "select * from " + tableName;
            rs = stmt.executeQuery(sql);

            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();
            for (int i = 1; i < columnCount + 1; i++) {
                columns.add(rsmd.getColumnLabel(i));
            }

        } catch (ConfigurationException e) {
            logger.error("配置异常 !", e);
        } catch (PropertyVetoException e) {
            logger.error("属性异常 !", e);
        } catch (SQLException e) {
            logger.error("SQL 异常 !", e);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                    rs = null;
                } catch (SQLException e) {
                    logger.error("SQL 异常 !", e);
                }
            }

            if (stmt != null) {
                try {
                    stmt.close();
                    stmt = null;
                } catch (SQLException e) {
                    logger.error("SQL 异常 !", e);
                }
            }

            if (conn != null) {
                try {
                    conn.close();
                    conn = null;
                } catch (SQLException e) {
                    logger.error("SQL 异常 !", e);
                }
            }
        }

        return columns;
    }

    /**
     * 同构数据库存储过程迁移
     *
     * @param srcDatabase       源数据库
     * @param srcCatalog        catalog ( null 为全部 )
     * @param srcSchemaPattern  schema 模式 ( null 为全部 )
     * @param srcNamePattern    名称模式
     * @param destDatabase      目标数据库
     * @param destCatalog       catalog ( null 为全部 )
     * @param destSchemaPattern schema 模式 ( null 为全部 )
     * @param otype             对象类型 ( 存储过程、视图 )
     * @param exportToFiles     是否导出文件 ( 每过程一个文件, 文件名为过程名 )
     * @param exportPath        导出文件路径 ( null 为工程路径中的 DatabaseObjects 目录中 )
     * @param migrate           是否执行迁移 ( 即在目标数据库建立对象 )
     */
    public static void SybaseObjectsMigrate(String srcDatabase, String srcCatalog, String srcSchemaPattern, String srcNamePattern, String destDatabase, String destCatalog, String destSchemaPattern, ObjectType otype, boolean exportToFiles, String exportPath, boolean migrate) {

        Connection srcConn = null;
        Connection destConn = null;
        Statement stmt = null;
        ResultSet rs = null;

        Map<String, String> objects = new HashMap<String, String>();
        List<String> objsName = new ArrayList<String>();
        String suffix = ".sql";
        StringBuffer content = null;

        try {
            // 获取源数据库 - 连接对象
            srcConn = ConnectionPool.getConnection(srcDatabase, ConfigParserUtils.getDatabaseConfiguration(srcDatabase));

            // 分析对象名称
            logger.info("@@@ 分析源数据库对象...");
            DatabaseMetaData srcDatabaseMetaData = srcConn.getMetaData();
            switch (otype) {
                case PROCEDURE:
                    rs = srcDatabaseMetaData.getProcedures(srcCatalog, srcSchemaPattern, srcNamePattern);
                    while (rs.next()) {
                        objsName.add(rs.getString(3));
                    }
                    break;
                case VIEW:
                    rs = srcDatabaseMetaData.getTables(srcCatalog, srcSchemaPattern, srcNamePattern, new String[]{"VIEW"});
                    while (rs.next()) {
                        objsName.add(rs.getString(3));
                    }
                    break;
                default:
                    break;
            }


            // 提取对象内容
            logger.info("@@@ 提取源数据库对象内容...");
            for (String objName : objsName) {
                String name = (otype == ObjectType.PROCEDURE) ? objName.substring(0, objName.indexOf(";")) : objName;
                String sql = "select comms.text from sysobjects objs join syscomments comms on objs.id=comms.id where objs.name='" + name + "' order by colid";
                stmt = srcConn.createStatement();
                rs = stmt.executeQuery(sql);

                content = new StringBuffer();
                while (rs.next()) {
                    content.append(rs.getString(1));
                }
                objects.put(name, content.toString());
            }

            // 文件导出
            if (exportToFiles) {
                logger.info("@@@ 导出对象文件...");
                for (String name : objects.keySet()) {
                    File file = new File((exportPath != null ? (exportPath + "/" + name + suffix) : ("DatabaseObjects/" + name + suffix)));
                    try {
                        FileUtils.writeStringToFile(file, objects.get(name));
                        logger.info("@@@ 对象: {} 已导出至文件: {}", name, file.getAbsolutePath());
                    } catch (IOException e) {
                        logger.error("IO 异常 !", e);
                    }
                }
            }

            // 迁移
            if (migrate) {
                logger.info("@@@ 迁移对象...");
                destConn = ConnectionPool.getConnection(destDatabase, ConfigParserUtils.getDatabaseConfiguration(destDatabase));
                for (String name : objects.keySet()) {
                    int lc = executeUpdate(destConn, objects.get(name));
                    logger.info("@@@ 对象 {} 迁移{} !", name, (lc < 0 ? "失败" : "成功"));
                }
            }

        } catch (ConfigurationException e) {
            logger.error("配置异常 !", e);
        } catch (PropertyVetoException e) {
            logger.error("属性异常 !", e);
        } catch (SQLException e) {
            logger.error("SQL 异常 !", e);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                    rs = null;
                } catch (SQLException e) {
                    logger.error("SQL 异常 !", e);
                }
            }

            if (stmt != null) {
                try {
                    stmt.close();
                    stmt = null;
                } catch (SQLException e) {
                    logger.error("SQL 异常 !", e);
                }
            }

            if (srcConn != null) {
                try {
                    srcConn.setAutoCommit(true);
                    srcConn.close();
                    srcConn = null;
                } catch (SQLException e) {
                    logger.error("SQL 异常 !", e);
                }
            }

            if (destConn != null) {
                try {
                    destConn.setAutoCommit(true);
                    destConn.close();
                    destConn = null;
                } catch (SQLException e) {
                    logger.error("SQL 异常 !", e);
                }
            }
        }
    }


}
