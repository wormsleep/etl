package zw.wormsleep.tools.etl.loader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zw.wormsleep.tools.etl.ETLExtractor;
import zw.wormsleep.tools.etl.ETLLoader;
import zw.wormsleep.tools.etl.ETLTransformer;
import zw.wormsleep.tools.etl.config.LoadConfig;
import zw.wormsleep.tools.etl.database.ConnectionPool;
import zw.wormsleep.tools.etl.database.DatabaseHelper;

import java.beans.PropertyVetoException;
import java.sql.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DatabaseLoader implements ETLLoader {

    final Logger logger = LoggerFactory.getLogger(DatabaseLoader.class);

    private LoadConfig loadConfig;

    public DatabaseLoader(LoadConfig loadConfig) {
        this.loadConfig = loadConfig;
    }

    @Override
    public void load(ETLExtractor extractor,
                     ETLTransformer transformer) {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        PreparedStatement pstmt = null;
        int batchSize = loadConfig.getBatchSize();
        Map<Integer, String> params = new HashMap<Integer, String>();
        Map<String, Integer> types = new HashMap<String, Integer>();
        String dbType = loadConfig.getDatabaseType();
        String database = loadConfig.getDatabase();
        Map<String, String> poolConfig = loadConfig.getDatabaseConfiguration();
        String table = loadConfig.getTable();

        /**
         * 关于目标表字段生成的处理需要说明下
         * 1. 为了兼容目标表字段的配置化和简化处理
         * 2. 若采用配置化（即在 columns 节点下配置 column 子节点）可以直接按配置生成 fields、updatefields
         * 3. 若采用简化（即在 output 节点下配置 selectsql keyfields nonupdatefields 子节点）就需要通用数据库元数据间接生成 fiedls、updatefields
         *
         * 通过数据库元数据生成 fields、updatefields 步骤
         * 1. 通过 selectSQL 在语句末尾添加 and 1=0 获取目标表字段元数据信息
         * 2. 通过元数据同时结合 keyfields 和 nonupdatefields 列表生成 fields、updatefields
         */
        Map<String, Boolean> fields = loadConfig.getFields();

        boolean ignoreUpdate = loadConfig.ignoreUpdate();


        /**
         * selectSQL 的获取或生成判断流程
         *
         * 1. 若配置中指定为表对表拷贝，则默认生成
         * 2. 若配置中已定义 <columns>...</columns> 则按配置生成
         * 3. 若无配置则通过根据 selectSQL 生成
         *
         */
        String selectSQL = loadConfig.getSelectSQL();

        // 防呆设计 - 在语句末尾添加 and 1=0
        if (selectSQL != null) {
            selectSQL += " and 1=0";
        }

        // 是否需要通过 selectSQL 生成 fields
        boolean makeFieldsFromSelectSQL = (selectSQL != null && fields.size() == 0) ? true : false;

        boolean isTable2Table = loadConfig.tableToTable();
        if (isTable2Table) {
            selectSQL = "select * from " + table + " where 1=0 ";
            logger.info("@@@ 表对表拷贝...");
        } else {
            // 若存在字段定义，则按定义处理，反之按 selectSQL 处理
            if (fields.size() > 0) {
                selectSQL = DatabaseHelper.getLoaderSelectSQL(table, fields);
            }
        }
        // 是否需要在采集前清空目标表
        boolean truncateTableBeforeLoad = loadConfig.truncateTableBeforeLoad();


        logger.info("@@@ 目的表 Select SQL: {}", selectSQL);

        try {
            // 连接数据库
            conn = ConnectionPool.getConnection(database, poolConfig);
            // 关闭自动提交
            conn.setAutoCommit(false);
            // *** 若进行表对表拷贝, 则首先清除目标表
            if (isTable2Table || truncateTableBeforeLoad) {
                DatabaseHelper.executeUpdate(conn, "truncate table " + table);
                logger.info("@@@ truncate table {}", table);
            }
            // 获取数据类型集合
            stmt = conn.createStatement();
            stmt.setFetchDirection(ResultSet.FETCH_FORWARD);
            stmt.setFetchSize(1);
            rs = stmt.executeQuery(selectSQL);
            ResultSetMetaData rsd = rs.getMetaData();
            int columnCount = rsd.getColumnCount();
            Map<String, Boolean> _fields = new HashMap<String, Boolean>();
            for (int i = 1; i <= columnCount; i++) {
                types.put(rsd.getColumnLabel(i), rsd.getColumnType(i));
                // 若需要通过 selectSQL 生成 fields ，后面结合 keyFields 对象更新主键字段
                if (makeFieldsFromSelectSQL) {
                    _fields.put(rsd.getColumnLabel(i), false);
                }
                logger.debug("@@@ 序号 {} 表 {} 字段 {} 类型 {}", i, table,
                        rsd.getColumnLabel(i), rsd.getColumnType(i));
            }

            // 组装批量提交 SQL 语句。若字段是通过 selectSQL 生成的，需要更新主键字段信息
            List<String> keyFields = loadConfig.getKeyFields();
            List<String> nonUpdateFields = loadConfig.getNonUpdateFields();
            List<String> updateFields = loadConfig.getUpdateFields();

            if (makeFieldsFromSelectSQL) {
                for (String field : _fields.keySet()) {
                    if (keyFields.contains(field)) {
                        _fields.put(field, true);
                    }
                    if (!nonUpdateFields.contains(field)) {
                        updateFields.add(field);
                    }
                }
            } else {
                _fields = fields;
            }

            String sql = DatabaseHelper
                    .getBatchInsertSQL((isTable2Table ? null : dbType), table, _fields, updateFields, ignoreUpdate);
            logger.info("@@@ Insert SQL - 预处理 {} ", sql);
            // *****************
            // if(true) return;
            // ****************
            String regex = "(:(\\w+))";
            Pattern p = Pattern.compile(regex);
            Matcher m = p.matcher(sql);
            int idx = 1;
            while (m.find()) {
                // 参数名称可能有重复，使用序号来做Key
                params.put(new Integer(idx++), m.group(2)); // 得到参数表
            }
            // 创建 pre
            sql = sql.replaceAll(regex, "?");
            logger.info("@@@ SQL : {}", sql);
            logger.info("@@@ 准备 prepareStatement ...");
            pstmt = conn.prepareStatement(sql);
            logger.info("@@@ prepareStatement 已创建 !");
            // 获取遍历器
            long lcnt = 0; // 处理记录数

            Iterator<Map<String, Object>> iter = extractor.walker();

            Map<String, Object> data = null;

            long startTime = System.currentTimeMillis();

            while (iter.hasNext()) {
                data = iter.next();
                // 优化数据正确性
                if (data == null) {
                    continue;
                }
                // 数据转换
                transformer.transform(data);

                // 特殊处理 - 向 Oracle 数据库传输数据时, 倍增其大写字段内容
                Map<String, Object> convertedData = new HashMap<String, Object>();
                if (dbType.equalsIgnoreCase("oracle")) {
                    for (String key : data.keySet()) {
                        convertedData.put(key.toUpperCase(), data.get(key));
                    }
                } else {
                    convertedData = data;
                }

//				logger.debug("@@@ 待导入数据 {}", data);

                lcnt++;
                DatabaseHelper.fillParamters(pstmt, convertedData, params, types);

                pstmt.addBatch();
                if (lcnt % batchSize == 0) {
                    pstmt.executeBatch();
                    conn.commit(); // 提交
                    pstmt.clearBatch();
                    logger.info("已处理：{} 条 - ( {} )", lcnt, table);
                }

            }

            pstmt.executeBatch();
            conn.commit(); // 提交
            logger.info("已处理：{} 条 - ( {} )", lcnt, table);
            logger.info("共计：{} 条 - ( {} )", lcnt, table);

            long endTime = System.currentTimeMillis();
            long consuming = (endTime - startTime) / 1000;
            logger.info("耗时 : {} ", (consuming / 60) > 0 ? (String.valueOf(consuming / 60) + " 分钟") : (consuming > 0 ? consuming + " 秒" : String.valueOf(endTime - startTime) + " 毫秒"));
            logger.info("平均 : {} ", consuming > 0 ? ((((lcnt * 60) / (consuming * 10000)) > 0) ? String.valueOf((lcnt * 60) / (consuming * 10000)) + " 万条/分钟" : String.valueOf(lcnt / consuming) + " 条/秒") : ((lcnt - (endTime - startTime) > 0 ? String.valueOf(lcnt / (endTime - startTime)) : "小于 1") + " 条/毫秒"));
        } catch (SQLException e) {
            logger.error("SQL 异常 !", e);
        } catch (PropertyVetoException e) {
            logger.error("属性异常 !", e);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                    rs = null;
                    logger.debug("@@@ [ 目标数据库 ] 销毁 ResultSet 成功!");
                } catch (SQLException e) {
                    logger.error("SQL 异常 !", e);
                }
            }

            if (stmt != null) {
                try {
                    stmt.close();
                    stmt = null;
                    logger.debug("@@@ [ 目标数据库 ] 销毁 Statement 成功!");
                } catch (SQLException e) {
                    logger.error("SQL 异常 !", e);
                }
            }

            if (pstmt != null) {
                try {
                    pstmt.close();
                    pstmt = null;
                    logger.debug("@@@ [ 目标数据库 ] 销毁 PrepareStatement 成功!");
                } catch (SQLException e) {
                    logger.error("SQL 异常 !", e);
                }
            }

            if (conn != null) {
                try {
                    conn.setAutoCommit(true);
                    conn.close();
                    conn = null;
                    logger.debug("@@@ [ 目标数据库 ] 销毁 Connection 成功!");
                } catch (SQLException e) {
                    logger.error("SQL 异常 !", e);
                }
            }
        }
    }


}
