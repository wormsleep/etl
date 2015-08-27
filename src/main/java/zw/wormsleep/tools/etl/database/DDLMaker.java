package zw.wormsleep.tools.etl.database;

import java.util.List;
import java.util.Map;

public interface DDLMaker {
	/**
	 * DDL - CREATE TABLE
	 * @param tableName 表名
	 * @param columnsMetadata 列元数据集
	 * @param typeConverter 数据类型转换器
	 * @return CREATE TABLE(...) 语句
	 */
	String createTable(String tableName, Map<String, Map<String, Object>> columnsMetadata, TypeConverter typeConverter, String primarykeyName, List<String> primarykeys);
}
