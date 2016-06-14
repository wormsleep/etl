package zw.wormsleep.tools.etl.config;

import java.util.Map;

public interface LoadConfig {
	Map<String, Boolean> getFields(); // 目标输出加载字段集合
	Map<String, Boolean> getKeyFields(); // 目标输出加载主键字段集合
	Map<String, Boolean> getUpdateFields(); // 目标输出加载更新字段集合
	String getOutputType(); // 目标输出类型
	String getDatabase(); // 目标数据库节点名称
	String getDatabaseType(); // 目标数据库类型
	Map<String, String> getDatabaseConfiguration(); // 目标数据库配置集合 ( c3p0 )
	String getTable(); // 目标输出数据库表名
	String getSeparator();  // 目标输出文件文本域分隔符
	String getEncoding(); // 目标输出文件编码
	String getFileType(); // 目标输出文件类型
	String getTemplate(); // 目标输出文件模板 ( 目前仅针对 Excel 文件输出 )
	String getTemplateCollection(); // GExcel 数据集合名称 ( 例如: data1 )
	int getMaxRowsNumberPerFile(); // 目标输出文件分割最大行数 ( 即按最大行数进行数据分割生成多文件 )
	boolean withHeader(); // 目标输出是否带列头
	String getCatalog(); // catalog ( 表对表批量传输使用 )
	String getSchemaPattern(); // schema 模式 ( 表对表批量传输使用 )
	boolean getAutoCreateTable(); // 是否自动创建目标数据库表 ( 表对表批量传输使用 )
	boolean getTransmitData(); // 是否传输数据至目标数据库表 ( 表对表批量传输使用 )
	int getThreadCount(); // 线程数  ( 表对表批量传输使用 )
	int getBatchSize(); // 批量处理数  ( 数据库 )
	boolean truncateTableBeforeLoad(); // 导入数据库前是否先清除表数据
	boolean ignoreUpdate(); // 导入数据库时是否在 if exists update else insert 语句中忽略 update 操作
	boolean tableToTable(); // 是否表对表拷贝。即是否在采集时先将目标表进行 truncate 处理
	String getSelectSQL(); // 获取加载表字段结构的 select 语句。注意必须在语句尾加上 where 1=0 以仅抓取结构
}
