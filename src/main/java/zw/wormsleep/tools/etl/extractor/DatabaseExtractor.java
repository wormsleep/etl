package zw.wormsleep.tools.etl.extractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zw.wormsleep.tools.etl.ETLExtractor;
import zw.wormsleep.tools.etl.config.ExtractConfig;
import zw.wormsleep.tools.etl.database.ConnectionPool;
import zw.wormsleep.tools.etl.database.DatabaseHelper;

import java.beans.PropertyVetoException;
import java.sql.*;
import java.util.*;

public class DatabaseExtractor implements ETLExtractor {
    final Logger logger = LoggerFactory.getLogger(DatabaseExtractor.class);

    private ExtractConfig extractConfig;
    private Map<String, String> parameters;
    private String sql;
    private Connection conn;
    private Statement stmt;
    private ResultSet rs;
    private List<String> fields = new ArrayList<String>();
    private int fetchSize;
    private boolean columnNameToLowerCase;

    public DatabaseExtractor(ExtractConfig extractConfig) {
        this.extractConfig = extractConfig;
        initial();
    }

    public DatabaseExtractor(ExtractConfig extractConfig,
                             Map<String, String> parameters) {
        this.parameters = parameters;
        this.extractConfig = extractConfig;
        initial();
    }

    public DatabaseExtractor(String sql, ExtractConfig extractConfig) {
        this.sql = sql;
        this.extractConfig = extractConfig;
        initial();
    }

    private void initial() {

        String table = extractConfig.getTable();
        fetchSize = extractConfig.getFetchSize();
        columnNameToLowerCase = extractConfig.columnNameToLowerCase();

        if (table != null && !table.equals("")) {
            sql = "select * from " + table;
        } else {
            if (sql == null) {
                sql = extractConfig.getSQL();
                logger.info("@@@ Config SQL: \n {}", sql);
                sql = DatabaseHelper.getReplacedSQL(sql, parameters);
            }
        }

        logger.info("@@@ Parsed SQL: \n {}", sql);

        String database = extractConfig.getDatabase();
        Map<String, String> poolConfig = extractConfig
                .getDatabaseConfiguration();

        try {
            conn = ConnectionPool.getConnection(database, poolConfig);
            stmt = conn.createStatement();
            // 由于是抽取数据故设置读取方向为向前每次读取固定记录数据
            stmt.setFetchDirection(ResultSet.FETCH_FORWARD);
            if (fetchSize > 1) {
                stmt.setFetchSize(fetchSize);
            }
            rs = stmt.executeQuery(sql);

            ResultSetMetaData rsd = rs.getMetaData();
            int columnCount = rsd.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                String field = rsd.getColumnLabel(i);
                fields.add(field);
            }
        } catch (PropertyVetoException e) {
            logger.error("属性异常 !", e);
        } catch (SQLException e) {
            logger.error("SQL 异常 !", e);
        }

    }

    @Override
    public Iterator<Map<String, Object>> walker() {

        return new Walker();
    }

    private class Walker implements Iterator<Map<String, Object>> {

        @Override
        public boolean hasNext() {
            boolean result = false;

            try {
                result = rs.next();

                if (!result) {
                    if (rs != null) {
                        rs.close();
                        rs = null;
                        logger.debug("@@@ [ 源数据库 ] 销毁 ResultSet 成功!");
                    }

                    if (stmt != null) {
                        stmt.close();
                        stmt = null;
                        logger.debug("@@@ [ 源数据库 ] 销毁 Statement 成功!");
                    }

                    if (conn != null) {
                        conn.close();
                        conn = null;
                        logger.debug("@@@ [ 源数据库 ] 销毁 Connection 成功!");
                    }
                }

            } catch (SQLException e) {
                logger.error("SQL 异常 !", e);
            }

            return result;
        }

        @Override
        public Map<String, Object> next() {
            Map<String, Object> map = new HashMap<String, Object>();

            int fieldCount = fields.size();

            for (int i = 0; i < fieldCount; i++) {
                try {
                    if (columnNameToLowerCase) {
                        map.put(fields.get(i).toLowerCase(), rs.getObject(i + 1));
                    } else {
                        map.put(fields.get(i), rs.getObject(i + 1));
                    }
                } catch (SQLException e) {
                    logger.error("SQL 异常 !", e);
                }
            }

            return map;
        }

        @Override
        public void remove() {
        }

    }

}
