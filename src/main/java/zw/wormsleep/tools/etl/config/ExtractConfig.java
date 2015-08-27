package zw.wormsleep.tools.etl.config;

import java.util.Map;

public interface ExtractConfig {
	String getInputType(); // 数据源类型
	String getSeparator();  // 文本域分隔符
	String getEncoding(); // 文件编码
	String getFileType(); // 文件类型
	String getDatabase(); // 数据库连接配置节点名称
	String getDatabaseType(); // 数据库类型
	String getSQL(); // SQL
	String getTable(); // 表名 ( 单一表 )
	Map<String, String> getDatabaseConfiguration(); // 数据库连接配置 ( c3p0 )
	Map<String, Integer> getIndexedColumns(); // 索引列集合 ( 来源一般为文本文件或 Excel 无列头文件 )
	boolean hasHeader(); // 文件是否有列头
	Map<String, String> getCheckColumns(); // 需检查或匹配的列集合
	int getMaxHeaderCheckRows();  // 列头匹配检查的最大行数
	int getRowStart(); // 抽取源数据起始行号 ( 目前仅针对文件类型 )
	int getRowEnd(); // 抽取源数据终止行号 ( 目前仅针对文件类型 )
	String getXpath(); // XML 文件抽取数据库节点路径
	String getCatalog(); // 源数据库 catalog ( 表对表批量传输使用 )
	String getSchemaPattern(); // 源数据库 schema 模式  ( 表对表批量传输使用 )
	String getTablePattern(); // 源数据库 table 模式 ( 表对表批量传输使用 )
	int getFetchSize(); // 源数据库 table 模式 ( 表对表批量传输使用 )
}
