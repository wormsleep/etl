package zw.wormsleep.tools.etl.database.converter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zw.wormsleep.tools.etl.database.TypeConverter;

import java.sql.Types;
import java.util.Map;

public class Sybase2SybaseTypeConverter implements TypeConverter {
    final Logger logger = LoggerFactory
            .getLogger(Sybase2SybaseTypeConverter.class);

    private final String SPACE = " ";

    @Override
    public String convert(Map<String, Object> type) {
        StringBuffer sbuf = new StringBuffer();

        String columnName = (String) type.get("COLUMN_NAME");
        Integer dataType = (Integer) type.get("DATA_TYPE");
        String typeName = (String) type.get("TYPE_NAME");
        Integer columnSize = (Integer) type.get("COLUMN_SIZE");
        Integer decimalDigits = (Integer) type.get("DECIMAL_DIGITS");
        Integer nullable = (Integer) type.get("NULLABLE");
        String columnDef = (String) type.get("COLUMN_DEF");

        // column_name
        sbuf.append(SPACE + columnName.trim() + SPACE);
        // column_name varchar
        // 目前发现一个特例 - 自增长列 IDENTITY 解决方案暂定移除特殊性, 即仅提取字段前面的字段名部分
        if (typeName.indexOf(" ") > 0) {
            sbuf.append(SPACE + typeName.substring(0, typeName.indexOf(" ")).trim());
        } else {
            sbuf.append(SPACE + typeName);
        }
        // 判断是否需要带括号
        // column_name varchar(40)
        switch (dataType) {
            case Types.BINARY:
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.VARBINARY:
                sbuf.append("(" + columnSize + ")" + SPACE);
                break;
            case Types.NUMERIC:
            case Types.DECIMAL:
                sbuf.append("(" + columnSize + "," + decimalDigits + ")" + SPACE);
            default:
                sbuf.append(SPACE);
        }
        // column_name varchar(40) default newid()
        if (columnDef != null) {
            sbuf.append(SPACE + "DEFAULT" + SPACE + columnDef + SPACE);
        }
        // column_name varchar(40) default newid() null
        sbuf.append(SPACE + (nullable == 0 ? "NOT NULL" : "NULL") + SPACE);
        // column_name varchar(40)

        return sbuf.toString();
    }

}
