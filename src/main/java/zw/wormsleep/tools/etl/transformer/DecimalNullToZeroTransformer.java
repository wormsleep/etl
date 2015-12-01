package zw.wormsleep.tools.etl.transformer;

import java.io.File;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;

import zw.wormsleep.tools.etl.ETLTransformer;
import zw.wormsleep.tools.etl.config.SimpleTransformConfig;
import zw.wormsleep.tools.etl.transformer.SimpleETLTransformer;

/**
 * 数值类型转换器 - 源数值为空值时设定零值
 * 
 * @author zhaowei
 *
 */
public class DecimalNullToZeroTransformer implements ETLTransformer {
	
	private SimpleETLTransformer _transformer;
	private String[] fieldname;
	
	public DecimalNullToZeroTransformer(String businessType, String[] fieldname) throws ConfigurationException {
		this._transformer = new SimpleETLTransformer(new SimpleTransformConfig(businessType));
		this.fieldname = fieldname;
	}

	public DecimalNullToZeroTransformer(String businessType, String[] fieldname, File configuration) throws ConfigurationException {
		this._transformer = new SimpleETLTransformer(new SimpleTransformConfig(businessType, configuration));
		this.fieldname = fieldname;
	}

	@Override
	public void transform(Map<String, Object> row) {
		_transformer.transform(row);
		
		int len = fieldname.length;
		for(int i=0;i<len; i++) {
			String key = fieldname[i];
			String value = (String)row.get(key);
			if(value.isEmpty()) {
				row.put(key, "0");
			}
		}
	}
}
