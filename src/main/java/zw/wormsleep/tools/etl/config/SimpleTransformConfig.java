package zw.wormsleep.tools.etl.config;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration;

import org.apache.commons.lang3.time.DateUtils;
import zw.wormsleep.tools.etl.transformer.DateFormatter;
import zw.wormsleep.tools.etl.transformer.Formatter;
import zw.wormsleep.tools.etl.transformer.NumberFormatter;
import zw.wormsleep.tools.etl.utils.ConfigParserUtils;

public class SimpleTransformConfig implements TransformConfig {

	final String NODE_FIELD = "field";
	final String NODE_MAPPING_ITEM = "mapping.item";
	final String NODE_MAPPING_ITEM_KEY = "key";
	final String NODE_MAPPING_ITEM_VALUE = "value";

	final String PROP_TYPE = "[@type]";
	final String PROP_VALUE = "[@value]";

	final String TYPE_AUTO_GENERATE = "auto-generate";
	final String TYPE_CONSTANT = "constant";
	final String TYPE_MAP = "map";
	final String TYPE_FORMAT = "format";

	final String TYPE_AUTO_GENERATE_UUID = "uuid";
	final String TYPE_AUTO_GENERATE_DATE = "date";
	final String TYPE_AUTO_GENERATE_DATETIME = "datatime";

	public final String YYYY_MM_DD = "yyyy-MM-dd";
	public final String YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss";
	public final String[] PARSEPATTERNS = new String[] { "yyyy-MM",
			"yyyyMM", "yyyy/MM", "yyyyMMdd", "yyyy-MM-dd", "yyyy/MM/dd",
			"yyyyMMddHHmmss", "yyyy-MM-dd HH:mm:ss", "yyyy/MM/dd HH:mm:ss" };

	private HierarchicalConfiguration business;

	public SimpleTransformConfig(String businessType)
			throws ConfigurationException {
		business = ConfigParserUtils.getResourceConfiguration(businessType);
	}

	public SimpleTransformConfig(String businessType, File configuration)
			throws ConfigurationException {
		business = ConfigParserUtils.getResourceConfiguration(businessType,
				configuration);
	}

	@Override
	public Map<String, Object> getTransformColumns() {
		Map<String, Object> transformColumns = new HashMap<String, Object>();

		List<HierarchicalConfiguration> columns = ConfigParserUtils
				.getColumnConfiguration(business);
		String type, field;
		Object value = null;
		for (HierarchicalConfiguration column : columns) {
			type = column.getString(PROP_TYPE);
			// 仅对需要转换的列进行处理
			if (type != null) {
				field = column.getString(NODE_FIELD);

				if (type.equalsIgnoreCase(TYPE_AUTO_GENERATE)) { // 自动生成列
					value = getAutoGenerateValue(column.getString(PROP_VALUE));
				} else if (type.equalsIgnoreCase(TYPE_CONSTANT)) { // 定值列
					value = column.getString(PROP_VALUE);
				} else if (type.equalsIgnoreCase(TYPE_MAP)) { // 键-值对列
					value = getMappingValue(ConfigParserUtils
							.getConfigurationList(column, NODE_MAPPING_ITEM));
				} else if(type.equalsIgnoreCase(TYPE_FORMAT)) {
					value = getFormatter(column.getString(PROP_VALUE));
				} else {
					value = null;
				}

				if (value != null) {
					transformColumns.put(field, value);
				}
			}

		}

		return transformColumns;
	}

	private Object getAutoGenerateValue(String type) {
		Object obj = null;

		SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		if (type.equalsIgnoreCase(TYPE_AUTO_GENERATE_UUID)) {
			obj = UUID.randomUUID().toString().replaceAll("-", "");
		} else if (type.equalsIgnoreCase(TYPE_AUTO_GENERATE_DATE)) {
			obj = sdf1.format(Calendar.getInstance().getTime());
		} else if (type.equalsIgnoreCase(TYPE_AUTO_GENERATE_DATETIME)) {
			obj = sdf2.format(Calendar.getInstance().getTime());
		}

		return obj;
	}

	private Map<String, String> getMappingValue(
			List<HierarchicalConfiguration> mapping) {
		Map<String, String> map = new HashMap<String, String>();

		String key, value;
		for (HierarchicalConfiguration item : mapping) {
			key = item.getString(NODE_MAPPING_ITEM_KEY);
			value = item.getString(NODE_MAPPING_ITEM_VALUE);

			map.put(key, value);
		}

		return map;
	}

	/**
	 * 获取格式化器
	 * @param type 配置文件节点 column 之 value 属性
	 * @return
	 */
	private Formatter getFormatter(String type) {
		Formatter formatter = null;
		Pattern number = Pattern.compile("^[number|int].*");

		if(type.equalsIgnoreCase("date") || type.equalsIgnoreCase("datetime")) {
			formatter = new DateFormatter();
		} else if(number.matcher(type).matches()) {
			String[] nc = type.split("-");
			String nt = nc[0];
			if(nt.equalsIgnoreCase("number")) {

				if(nc.length > 2) {
					formatter = new NumberFormatter(nc[1], nc[2]);
				} else if(nc.length > 1) {
					formatter = new NumberFormatter(nc[1], "2");
				} else {
					formatter = new NumberFormatter(null, "2");
				}

			} else if(nt.equalsIgnoreCase("int")) {
				formatter = new NumberFormatter(null, "0");
			}
		}

		return formatter != null ? formatter : new Formatter() {
			@Override
			public Object format(Object value) {
				return value;
			}
		};
	}



}
