package zw.wormsleep.tools.etl.database;

import java.util.Map;

public interface TypeConverter {
    /**
     * 对同构或异构数据的字段类型进行 DDL SQL 级转换
     *
     * @param type
     * @return DDL SQL 字段建立信息 例如: columnName varchar(40) default newid() null
     */
    String convert(Map<String, Object> type);
}
