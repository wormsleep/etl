package zw.wormsleep.tools.etl.database.converter;

import java.sql.Types;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zw.wormsleep.tools.etl.database.TypeConverter;

public class Sybase2OracleTypeConverter implements TypeConverter {
	final Logger logger = LoggerFactory
			.getLogger(Sybase2OracleTypeConverter.class);
	
	private final String SPACE = " ";
	
	public Sybase2OracleTypeConverter() {}

	@Override
	public String convert(Map<String, Object> type) {
		StringBuffer columnDDL= new StringBuffer();

		String columnName = (String) type.get("COLUMN_NAME");
		Integer dataType = (Integer) type.get("DATA_TYPE");
		String typeName = (String) type.get("TYPE_NAME");
		Integer columnSize = (Integer) type.get("COLUMN_SIZE");
		Integer decimalDigits = (Integer) type.get("DECIMAL_DIGITS");
		Integer nullable = (Integer) type.get("NULLABLE");
		String columnDef = (String) type.get("COLUMN_DEF");

		// column_name
		columnDDL.append(SPACE + columnName + SPACE);
		// column_name varchar
		switch (dataType) {
		// 目前发现 Sybase 字段为 varchar 而 Oracle 字段为 varchar2 时会出现超出 Oracle 字段同长度值, 因此转换定义为 nvarchar2 
		case Types.VARCHAR:
			columnDDL.append(SPACE + "NVARCHAR2");
			break;
		case Types.BIGINT:
			columnDDL.append(SPACE + "NUMBER");
			break;
		case Types.BINARY:
		case Types.VARBINARY:
			columnDDL.append(SPACE + "RAW");
			break;
		case Types.DECIMAL:
			columnDDL.append(SPACE + "NUMBER");
			break;
		case Types.INTEGER:
			columnDDL.append(SPACE + "INTEGER");
			break;
		case Types.LONGVARBINARY:
			columnDDL.append(SPACE + "LOGN RAW");
			break;
		case Types.LONGVARCHAR:
			columnDDL.append(SPACE + "LONG");
			break;
		case Types.TIME: 
		case Types.TIMESTAMP:
			columnDDL.append(SPACE + "DATE");
			break;
		default:
			columnDDL.append(SPACE + typeName);
		}
		
		// 判断是否需要带括号
		// column_name varchar(40)
		switch (dataType) {
		case Types.BIGINT:
			columnDDL.append("(38,0)" + SPACE);
			break;
		case Types.BINARY:
		case Types.CHAR:
		case Types.VARCHAR:
		case Types.VARBINARY:
			columnDDL.append("(" + columnSize + ")" + SPACE);
			break;
		case Types.NUMERIC:
		case Types.DECIMAL:
			columnDDL.append("(" + columnSize + "," + decimalDigits + ")" + SPACE);
			break;
		default:
			columnDDL.append(SPACE);
		}
		// column_name varchar(40) default newid()
		if (columnDef != null) {
			if(columnDef.trim().equalsIgnoreCase("newid()")) {
				columnDDL.append(SPACE + "DEFAULT" + SPACE + "sys_guid()" + SPACE);
			} else if(columnDef.trim().equalsIgnoreCase("getdate()")) {
				columnDDL.append(SPACE + "DEFAULT" + SPACE + "sysdate" + SPACE);
			} else {
				columnDDL.append(SPACE + "DEFAULT" + SPACE + columnDef.trim() + SPACE);
			}
		}
		// column_name varchar(40) default newid() null
		columnDDL.append(SPACE + (nullable == 0 ? "NOT NULL" : "NULL") + SPACE);
		// column_name varchar(40)

		return columnDDL.toString();
	}

}
