package zw.wormsleep.tools.etl.database.printer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zw.wormsleep.tools.etl.database.DatabaseMetadataPrinter;

import java.sql.*;

public class TableMetadataPrinter implements DatabaseMetadataPrinter {

    final Logger logger = LoggerFactory.getLogger(TableMetadataPrinter.class);

    private String catalog;
    private String schemaPattern;
    private String tableNamePattern;
    private String[] types;

    public TableMetadataPrinter(String catalog, String schemaPattern,
                                String tableNamePattern, String[] types) {
        this.catalog = catalog;
        this.schemaPattern = schemaPattern;
        this.tableNamePattern = tableNamePattern;
        this.types = (types != null) ? types : new String[]{"TABLE"};
    }

    @Override
    public void print(Connection conn, DatabaseMetaData dmd) {
        ResultSet rs = null;

        // 获取列描述对象
        try {
            // 使用时注意对于 Oracle 来说, schemaPattern 名称必须大写
            rs = dmd.getTables(catalog, schemaPattern, tableNamePattern, types);
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
                    System.out.print("<" + i + ">" + rs.getObject(i) + " [" + (rs.getObject(i) != null ? rs.getObject(i).getClass() : rs.getObject(i)) + "]\t");
                }
                System.out.println();
            }

        } catch (SQLException e) {
            logger.error("处理异常：" + e.getMessage());
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                    rs = null;
                } catch (SQLException e) {
                    logger.error("处理异常：" + e.getMessage());
                }
            }
        }

    }
}
