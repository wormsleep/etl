package zw.wormsleep.tools.etl.database.converter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zw.wormsleep.tools.etl.database.TypeConverter;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.Map;

public class Oracle2SybaseTypeConverter implements TypeConverter {
    final Logger logger = LoggerFactory
            .getLogger(Oracle2SybaseTypeConverter.class);

    private final String SPACE = " ";

    public Oracle2SybaseTypeConverter() {
    }

    @Override
    public String convert(Map<String, Object> type) {
        StringBuffer columnDDL = new StringBuffer();

        String columnName = (String) type.get("COLUMN_NAME");
        Integer dataType = ((BigDecimal) type.get("DATA_TYPE")).intValue();
        String typeName = (String) type.get("TYPE_NAME");
        Integer columnSize = ((BigDecimal) type.get("COLUMN_SIZE")).intValue();
        Integer decimalDigits = type.get("DECIMAL_DIGITS") != null ? ((BigDecimal) type.get("DECIMAL_DIGITS")).intValue() : null;
        Integer nullable = ((BigDecimal) type.get("NULLABLE")).intValue();
        String columnDef = (String) type.get("COLUMN_DEF");

        // column_name
        columnDDL.append(SPACE + columnName + SPACE);
        // column_name varchar
        switch (dataType) {
            case Types.BIGINT:
                columnDDL.append(SPACE + "BIGINT");
                break;
            case Types.BINARY:
                columnDDL.append(SPACE + "BINARY");
                break;
            case Types.VARBINARY:
                columnDDL.append(SPACE + "VARBINARY");
                break;
            case Types.DECIMAL:
                columnDDL.append(SPACE + "DECIMAL");
                break;
            case Types.INTEGER:
                columnDDL.append(SPACE + "INT");
                break;
            case Types.LONGVARBINARY:
                columnDDL.append(SPACE + "LOGN RAW");
                break;
            case Types.LONGVARCHAR:
                columnDDL.append(SPACE + "TEXT");
                break;
            case Types.TIME:
                columnDDL.append(SPACE + "TIME");
                break;
            case Types.TIMESTAMP:
                columnDDL.append(SPACE + "DATETIME");
                break;
            default:
                if (typeName.equals("NVARCHAR2")) {
                    columnDDL.append(SPACE + "NVARCHAR");
                } else {
                    columnDDL.append(SPACE + typeName);
                }
        }

        // 判断是否需要带括号
        // column_name varchar(40)
        switch (dataType) {
            case Types.BIGINT:
            case Types.BINARY:
            case Types.CHAR:
            case Types.NCHAR:
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.VARBINARY:
                columnDDL.append("(" + columnSize + ")" + SPACE);
                break;
            case Types.NUMERIC:
            case Types.DECIMAL:
                columnDDL.append("(" + columnSize + "," + decimalDigits + ")" + SPACE);
                break;
            default:
                if (typeName.equals("NVARCHAR2")) {
                    columnDDL.append("(" + columnSize + ")" + SPACE);
                } else {
                    columnDDL.append(SPACE);
                }

        }
        // column_name varchar(40) default newid()
        if (columnDef != null) {
            logger.debug("@@@ columnDef: {}", columnDef);
            if (columnDef.trim().equalsIgnoreCase("sys_guid()")) {
                columnDDL.append(SPACE + "DEFAULT" + SPACE + "newid()" + SPACE);
            } else if (columnDef.trim().equalsIgnoreCase("sysdate")) {
                columnDDL.append(SPACE + "DEFAULT" + SPACE + "getdate()" + SPACE);
            } else {
                columnDDL.append(SPACE + "DEFAULT" + SPACE + columnDef.trim() + SPACE);
            }
        }
        // column_name varchar(40) default newid() null
        columnDDL.append(SPACE + (nullable == 0 ? "NOT NULL" : "NULL") + SPACE);

        return columnDDL.toString();
    }

}
