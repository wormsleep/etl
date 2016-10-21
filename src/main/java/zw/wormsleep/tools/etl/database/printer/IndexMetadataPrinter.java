package zw.wormsleep.tools.etl.database.printer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zw.wormsleep.tools.etl.database.DatabaseMetadataPrinter;

import java.sql.*;

public class IndexMetadataPrinter implements DatabaseMetadataPrinter {
    final Logger logger = LoggerFactory.getLogger(IndexMetadataPrinter.class);

    private String catalog;
    private String schema;
    private String table;
    private boolean unique;
    private boolean approximate;

    public IndexMetadataPrinter(String catalog, String schema, String table, boolean unique, boolean approximate) {
        this.catalog = catalog;
        this.schema = schema;
        this.table = table;
        this.unique = unique;
        this.approximate = approximate;
    }

    @Override
    public void print(Connection conn, DatabaseMetaData dmd) {

        ResultSet rs = null;

        // 获取列描述对象
        try {
            // 使用时注意对于 Oracle 来说, schemaPattern 名称必须大写
            rs = dmd.getIndexInfo(catalog, schema, table, unique, approximate);
            // 获取结果集元数据对象
            ResultSetMetaData rsmd = rs.getMetaData();
            // 打印结果集列头信息
            int columnCount = rsmd.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                System.out.print("<" + i + ">" + rsmd.getColumnLabel(i) + "\t");
            }
            System.out.println();
            // 打印结果集内容信息
            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    System.out.print("<" + i + ">" + rs.getObject(i) + "\t");
                }
                System.out.println();
            }

        } catch (SQLException e) {
            logger.error("@@@ SQL 异常 !");
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                    rs = null;
                } catch (SQLException e) {
                    logger.error("@@@ SQL 异常 !");
                }
            }
        }
    }
}
