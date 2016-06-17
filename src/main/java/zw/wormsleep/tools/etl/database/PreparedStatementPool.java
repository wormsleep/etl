package zw.wormsleep.tools.etl.database;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zw.wormsleep.tools.etl.config.LoadConfig;

/**
 * 为了支持静态 PreparedStatement 对象集使用
 *
 * @author zhaowei
 */
public class PreparedStatementPool {
    static final Logger logger = LoggerFactory.getLogger(PreparedStatementPool.class);

    private static Map<String, Map<String, Object>> pool = new HashMap<String, Map<String, Object>>();

    private PreparedStatementPool() {
    }

    private static Map<String, Object> create(String businessType, LoadConfig loadConfig) {
        // 当前时间对应的毫秒数
        long startMillis = System.currentTimeMillis();

        Map<String, Object> pstmtObject = new HashMap<String, Object>();

        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        PreparedStatement pstmt = null;
        Map<Integer, String> params = new HashMap<Integer, String>();
        Map<String, Integer> types = new HashMap<String, Integer>();
        String dbType = loadConfig.getDatabaseType();
        String database = loadConfig.getDatabase();
        Map<String, String> poolConfig = loadConfig.getDatabaseConfiguration();
        String table = loadConfig.getTable();
        Map<String, Boolean> fields = loadConfig.getFields();
        List<String> updateFields = loadConfig.getUpdateFields();
        boolean ignoreUpdate = loadConfig.ignoreUpdate();
        String selectSQL = "";
        boolean isTable2Table = fields.size() < 1 ? true : false;
        if (isTable2Table) { // 对于未定义导入列，即表对表拷贝时
            selectSQL = "select * from " + table;
            logger.info("@@@ 表对表拷贝...");
        } else { // 对于已定义导入列情况
            selectSQL = DatabaseHelper.getLoaderSelectSQL(table, fields);
        }

        logger.debug("@@@ Select SQL: {}", selectSQL);

        try {
            // 连接数据库
            conn = ConnectionPool.getConnection(database, poolConfig);
            // 关闭自动提交
            conn.setAutoCommit(false);

            // 获取数据类型集合
            stmt = conn.createStatement();
            rs = stmt.executeQuery(selectSQL);
            ResultSetMetaData rsd = rs.getMetaData();
            int columnCount = rsd.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                types.put(rsd.getColumnLabel(i), rsd.getColumnType(i));
                logger.debug("@@@ 序号 {} 表 {} 字段 {} 类型 {}", i, table,
                        rsd.getColumnLabel(i), rsd.getColumnType(i));
            }
            // 获取参数集合
            // *** 若进行表对表拷贝
            if (isTable2Table) {
                fields = new HashMap<String, Boolean>();
                for (String field : types.keySet()) {
                    fields.put(field, false);
                }
            }

            String sql = DatabaseHelper
                    .getBatchInsertSQL(dbType, table, fields, updateFields, ignoreUpdate);
            logger.debug("@@@ Insert SQL - 预处理 {}", sql);
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
            pstmt = conn.prepareStatement(sql);

            // 组装返回用的 Connection 和 PreparedStatement
            pstmtObject.put("conn", conn);
            pstmtObject.put("pstmt", pstmt);
            pstmtObject.put("params", params);
            pstmtObject.put("types", types);

            // 当前时间对应的毫秒数
            long endMillis = System.currentTimeMillis();
            logger.info("@@@ [耗时: {} 毫秒]已创建 {} 业务类型的数据库处理预编译对象 !", (endMillis - startMillis), businessType);

        } catch (SQLException e) {
            logger.error("SQL 异常 !", e);
        } catch (PropertyVetoException e) {
            logger.error("属性异常 !", e);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                    rs = null;
                } catch (SQLException e) {
                    logger.error("处理异常：" + e.getMessage());
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
        }

        return pstmtObject;

    }

    public static Map<String, Object> getPreparedStatement(String businessType, LoadConfig loadConfig) {
        Map<String, Object> pstmtObject = pool.get(businessType);

        try {
            if (pstmtObject == null || ((Connection) pstmtObject.get("conn")).isClosed()) {
                pstmtObject = create(businessType, loadConfig);
                pool.put(businessType, pstmtObject);
            }
        } catch (SQLException e) {
            logger.error("SQL 异常 !", e);
        }

        return pstmtObject;
    }

    public static void release(String businessType) {
        Map<String, Object> pstmtObject = pool.get(businessType);

        if (pstmtObject != null) {
            Connection conn = (Connection) pstmtObject.get("conn");
            PreparedStatement pstmt = (PreparedStatement) pstmtObject.get("pstmt");

            if (conn != null) {
                try {
                    conn.close();
                    conn = null;
                } catch (SQLException e) {
                    logger.error("SQL 异常 !", e);
                }
            }

            if (pstmt != null) {
                try {
                    pstmt.close();
                    pstmt = null;
                } catch (SQLException e) {
                    logger.error("SQL 异常 !", e);
                }
            }
        }
    }


}
