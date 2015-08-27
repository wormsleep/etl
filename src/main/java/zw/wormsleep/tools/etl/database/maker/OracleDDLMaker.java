package zw.wormsleep.tools.etl.database.maker;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zw.wormsleep.tools.etl.database.DDLMaker;
import zw.wormsleep.tools.etl.database.TypeConverter;

public class OracleDDLMaker implements DDLMaker {
	final Logger logger = LoggerFactory.getLogger(OracleDDLMaker.class);

	public OracleDDLMaker() {
	}

	@Override
	public String createTable(String tableName,
			Map<String, Map<String, Object>> columnsMetadata,
			TypeConverter typeConverter, String primarykeyName,
			List<String> primarykeys) {
		StringBuffer tableDDL = new StringBuffer();

		// 起始部分
		tableDDL.append("create table " + tableName + "( \n");
		// 字段部分
		for (String columnName : columnsMetadata.keySet()) {
			Map<String, Object> metadata = columnsMetadata.get(columnName);
			String columnDDL = typeConverter.convert(metadata);
			tableDDL.append(columnDDL + "," + "\n");
		}

		// 主键部分
		String primarykeyDDL = makePrimarykey(primarykeyName, primarykeys);
		if (primarykeyDDL.length() > 0) {
			tableDDL.append(primarykeyDDL + "\n");
		} else {
			int lastCommaIndex = tableDDL.lastIndexOf(",");
			if (lastCommaIndex > 0) {
				tableDDL.deleteCharAt(lastCommaIndex);
			}
		}
		// 结尾部分
		tableDDL.append(" )" + "\n");

		return tableDDL.toString();
	}

	/**
	 * 生成 Sybase 数据库建表主键语句
	 * 
	 * @param primarykeyName
	 * @param primarykeys
	 * @return
	 */
	private String makePrimarykey(String primarykeyName,
			List<String> primarykeys) {
		StringBuffer ddl = new StringBuffer();
		if (primarykeyName != null && primarykeys != null
				&& primarykeys.size() > 0) {

			if (primarykeys != null && primarykeys.size() > 0) {
				ddl.append(" CONSTRAINT " + primarykeyName);
				ddl.append(" PRIMARY KEY (");
				for (String key : primarykeys) {
					ddl.append(key + ",");
				}
				int lastCommaIndex = ddl.lastIndexOf(",");
				if (lastCommaIndex > 0) {
					ddl.deleteCharAt(lastCommaIndex);
				}
				ddl.append(" ) ");
			}

		}
		return ddl.toString();
	}

}
