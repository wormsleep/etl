package zw.wormsleep.tools.etl.database.printer;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zw.wormsleep.tools.etl.database.DatabaseMetadataPrinter;

public class DatabaseProductInformationPrinter implements
		DatabaseMetadataPrinter {
	
	final Logger logger = LoggerFactory.getLogger(DatabaseProductInformationPrinter.class);
	
	public DatabaseProductInformationPrinter() {
		
	}

	@Override
	public void print(Connection conn, DatabaseMetaData dmd) {
		try {
			String databaseProductName = dmd.getDatabaseProductName();
			System.out.printf("[ databaseProductName ] : %s \n", databaseProductName);
			String databaseProductVersion = dmd.getDatabaseProductVersion();
			System.out.printf("[ databaseProductVersion ] : %s \n", databaseProductVersion);
			String driverName = dmd.getDriverName();
			System.out.printf("[ driverName ] : %s \n", driverName);
			String driverVersion = dmd.getDriverVersion();
			System.out.printf("[ driverVersion ] : %s \n", driverVersion);
			String catalogSeparator = dmd.getCatalogSeparator();
			System.out.printf("[ catalogSeparator ] : %s \n", catalogSeparator);
			String catalogTerm = dmd.getCatalogTerm();
			System.out.printf("[ catalogTerm ] : %s \n", catalogTerm);
			String searchStringEscape = dmd.getSearchStringEscape();
			System.out.printf("[ searchStringEscape ] : %s \n", searchStringEscape);
			
			int maxRowSize = dmd.getMaxRowSize();
			System.out.printf("[ maxRowSize ] : %d bytes \n", maxRowSize);
			int maxColumnsInTable = dmd.getMaxColumnsInTable();
			System.out.printf("[ maxColumnsInTable ] : %d \n", maxColumnsInTable);
			int maxConnections = dmd.getMaxConnections();
			System.out.printf("[ maxConnections ] : %d \n", maxConnections);
			int maxTablesInSelect = dmd.getMaxTablesInSelect();
			System.out.printf("[ maxTablesInSelect ] : %d \n", maxTablesInSelect);
		} catch (SQLException e) {
			logger.error("@@@ SQL 异常 !");
		}
			
		try {
			String SQLKeywords = dmd.getSQLKeywords(); // 这个 sybase 会报错
			System.out.printf("[ SQLKeywords ] : %s \n", SQLKeywords);
		} catch (SQLException e) {
			logger.error("@@@ 数据库不支持 {} 方法 !", "getSQLKeywords");
		}
		
		try {
			int default_holdability = dmd.getResultSetHoldability();
			System.out.printf("[ default Resultset holdability : %s \n ]", default_holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT ? "HOLD_CURSORS_OVER_COMMIT" : "CLOSE_CURSORS_AT_COMMIT");
		} catch (SQLException e) {
			logger.error("@@@ 数据库不支持 {} 方法 !", "getResultSetHoldability");
		}	
	}

}
