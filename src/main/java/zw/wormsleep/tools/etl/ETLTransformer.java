package zw.wormsleep.tools.etl;

import java.util.Map;

public interface ETLTransformer {
	void transform(Map<String, Object> row);
}
