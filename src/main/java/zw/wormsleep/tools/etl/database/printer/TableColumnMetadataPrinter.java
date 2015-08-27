package zw.wormsleep.tools.etl.database.printer;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zw.wormsleep.tools.etl.database.DatabaseMetadataPrinter;

public class TableColumnMetadataPrinter implements DatabaseMetadataPrinter {

	final Logger logger = LoggerFactory.getLogger(TableColumnMetadataPrinter.class);

	private String catalog;
	private String schemaPattern;
	private String tableNamePattern;
	private String columnNamePattern;

	/**
	 * 构造器
	 * @param catalog
	 * @param schemaPattern
	 * @param tableNamePattern
	 * @param columnNamePattern
	 */
	public TableColumnMetadataPrinter(String catalog, String schemaPattern,
			String tableNamePattern, String columnNamePattern) {
		this.catalog = catalog;
		this.schemaPattern = schemaPattern;
		this.tableNamePattern = tableNamePattern;
		this.columnNamePattern = columnNamePattern;
	}

	@Override
	public void print(Connection conn, DatabaseMetaData dmd) {
		ResultSet rs = null;

		// 获取列描述对象
		try {
			// 使用时注意对于 Oracle 来说, schemaPattern 名称必须大写
			rs = dmd.getColumns(catalog, schemaPattern, tableNamePattern,
					columnNamePattern);
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
					System.out.print("<" + i + ">" + rs.getObject(i) +"\t");
//					System.out.print("<" + i + ">" + rs.getObject(i) + " ["+(rs.getObject(i) != null ? rs.getObject(i).getClass() : rs.getObject(i))+"]\t");
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
