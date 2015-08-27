package zw.wormsleep.tools.etl.config;

import java.util.Map;

public interface TransformConfig {
	Map<String, Object> getTransformColumns(); // 转换列集合 ( 供默认转换器使用 )
}
