package zw.wormsleep.tools.etl.database.printer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zw.wormsleep.tools.etl.database.DatabaseMetadataPrinter;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class ProcedureMetadataPrinter implements DatabaseMetadataPrinter {
    final Logger logger = LoggerFactory
            .getLogger(ProcedureMetadataPrinter.class);
    boolean printComments = false;
    private String catalog;
    private String schemaPattern;
    private String procedureNamePattern;

    public ProcedureMetadataPrinter(String catalog, String schemaPattern,
                                    String procedureNamePattern) {
        this.catalog = catalog;
        this.schemaPattern = schemaPattern;
        this.procedureNamePattern = procedureNamePattern;
    }

    public ProcedureMetadataPrinter(String catalog, String schemaPattern,
                                    String procedureNamePattern, boolean printComments) {
        this.catalog = catalog;
        this.schemaPattern = schemaPattern;
        this.procedureNamePattern = procedureNamePattern;
        this.printComments = printComments;
    }

    @Override
    public void print(Connection conn, DatabaseMetaData dmd) {
        ResultSet rs = null;
        List<String> procedureName = new ArrayList<String>();
        // 获取列描述对象
        try {
            System.out.println(dmd.getDatabaseProductName());
            // 使用时注意对于 Oracle 来说, schemaPattern 名称必须大写
            rs = dmd.getProcedures(catalog, schemaPattern, procedureNamePattern);
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
                procedureName.add(rs.getString(3));
            }

            if (printComments) {
                printSybaseProcedureComments(conn, procedureName);
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

    /**
     * 打印 Sybase 数据库存储过程
     *
     * @param conn
     * @param procedureName
     */
    private void printSybaseProcedureComments(Connection conn,
                                              List<String> procedureName) {
        Statement stmt = null;
        ResultSet rs = null;
        String sql = null;
        StringBuffer sbuf = null;
        try {
            System.out.printf("######### 以下打印 %d 个存储过程定义内容 ... \n\n", procedureName.size());
            for (String name : procedureName) {
                sbuf = new StringBuffer();

                name = name.substring(0, name.indexOf(";"));

                System.out.printf("******************************** 分界线 ********************************* \n\n");
                System.out.printf("@@@ 开始打印存储过程 %s 定义内容 ... \n\n", name);

                stmt = conn.createStatement();
                sql = "select comms.text from sysobjects objs join syscomments comms on objs.id=comms.id where objs.name='" + name + "' and type='P' order by colid";
                rs = stmt.executeQuery(sql);

                while (rs.next()) {
                    sbuf.append(rs.getString(1));
                }

                System.out.println(sbuf.toString());

                System.out.printf("@@@ 存储过程 %s 定义内容打印结束 ! \n\n", name);
            }
            System.out.printf("######### 存储过程定义内容打印结束 ! \n\n");
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
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
                    logger.error("处理异常：" + e.getMessage());
                }
            }
        }
    }

}
