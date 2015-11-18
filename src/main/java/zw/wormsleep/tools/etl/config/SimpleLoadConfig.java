package zw.wormsleep.tools.etl.config;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration;

import zw.wormsleep.tools.etl.utils.ConfigParserUtils;

public class SimpleLoadConfig implements LoadConfig {

	final String PROP_KEY = "[@key]";
	final String NODE_NAME = "name";
	final String NODE_FIELD = "field";
	final String NODE_OUTPUT = "output";
	final String PROP_OUTPUT_TYPE = "output[@type]";
	final String NODE_OUTPUT_DATABASE = "output.database";
	final String PROP_OUTPUT_DATABASE_TYPE = "output.database[@type]";
	final String NODE_OUTPUT_TABLE = "output.table";
	final String NODE_OUTPUT_FILETYPE = "output.filetype";
	final String NODE_OUTPUT_SEPARATOR = "output.separator";
	final String NODE_OUTPUT_ENCODING = "output.encoding";
	final String NODE_OUTPUT_TEMPLATE = "output.template";
	final String NODE_OUTPUT_MAX_ROWS_PER_FILE = "output.maxrowsperfile";
	final String NODE_OUTPUT_WITH_HEADER = "output.withheader";
	final String NODE_OUTPUT_CATALOG = "output.catalog";
	final String NODE_OUTPUT_SCHEMA_PATTERN = "output.schemapattern";
	final String NODE_OUTPUT_AUTO_CREATE_TABLE = "output.autocreatetable";
	final String NODE_OUTPUT_TRANSMIT_DATA = "output.transmitdata";
	final String NODE_OUTPUT_THREAD_COUNT = "output.threadcount";
	final String NODE_OUTPUT_BATCH_SIZE = "output.batchsize";
	final String NODE_OUTPUT_TEMPLATE_COLLECTION = "templatecollection";
	
	private HierarchicalConfiguration business;
	private Map<String, String> database;

	public SimpleLoadConfig(String businessType) throws ConfigurationException {
		business = ConfigParserUtils.getResourceConfiguration(businessType);
		database = ConfigParserUtils.getDatabaseConfiguration(business.getString(NODE_OUTPUT_DATABASE));
	}

	public SimpleLoadConfig(String businessType, File configuration)
			throws ConfigurationException {
		business = ConfigParserUtils.getResourceConfiguration(businessType,
				configuration);
		database = ConfigParserUtils.getDatabaseConfiguration(business.getString(NODE_OUTPUT_DATABASE), configuration);
	}

	@Override
	public Map<String, Boolean> getFields() {
		Map<String, Boolean> fields = new LinkedHashMap<String, Boolean>();

		List<HierarchicalConfiguration> columns = ConfigParserUtils
				.getColumnConfiguration(business);

		for (HierarchicalConfiguration column : columns) {
			String field = column.getString(NODE_FIELD);
			String key = column.getString(PROP_KEY);

			if (key != null && key.equalsIgnoreCase("true")) {
				fields.put(field, true);
			} else {
				fields.put(field, false);
			}

		}

		return fields;
	}

	@Override
	public String getOutputType() {
		return business.getString(PROP_OUTPUT_TYPE, "database");
	}
	
	@Override
	public String getDatabase() {
		return business.getString(NODE_OUTPUT_DATABASE);
	}

	@Override
	public String getDatabaseType() {
		return business.getString(PROP_OUTPUT_DATABASE_TYPE);
	}

	@Override
	public String getTable() {
		return business.getString(NODE_OUTPUT_TABLE);
	}

	@Override
	public Map<String, String> getDatabaseConfiguration() {
		return database;
	}

	@Override
	public String getSeparator() {
		return ConfigParserUtils.getSeparator(business.getString(NODE_OUTPUT_SEPARATOR));
	}

	@Override
	public String getEncoding() {
		return business.getString(NODE_OUTPUT_ENCODING, "UTF-8");
	}

	@Override
	public String getFileType() {
		return business.getString(NODE_OUTPUT_FILETYPE);
	}

	@Override
	public String getTemplate() {
		return business.getString(NODE_OUTPUT_TEMPLATE);
	}

	@Override
	public int getMaxRowsNumberPerFile() {
		return business.getInt(NODE_OUTPUT_MAX_ROWS_PER_FILE, -1);
	}

	@Override
	public boolean withHeader() {
		return business.getBoolean(NODE_OUTPUT_WITH_HEADER, false);
	}

	@Override
	public String getCatalog() {
		String catalog = business.getString(NODE_OUTPUT_CATALOG);
		return (catalog != null && !catalog.equals("")) ? catalog : null;
	}

	@Override
	public String getSchemaPattern() {
		String schemaPattern = business.getString(NODE_OUTPUT_SCHEMA_PATTERN);
		return (schemaPattern != null && !schemaPattern.equals("")) ? schemaPattern : null;
	}

	@Override
	public boolean getAutoCreateTable() {
		return business.getBoolean(NODE_OUTPUT_AUTO_CREATE_TABLE, true);
	}

	@Override
	public boolean getTransmitData() {
		return business.getBoolean(NODE_OUTPUT_TRANSMIT_DATA, true);
	}

	@Override
	public int getThreadCount() {
		return business.getInt(NODE_OUTPUT_THREAD_COUNT, 1);
	}

	@Override
	public int getBatchSize() {
		return business.getInt(NODE_OUTPUT_BATCH_SIZE, 200);
	}

	@Override
	public String getTemplateCollection() {
		return business.getString(NODE_OUTPUT_TEMPLATE_COLLECTION, "data1");
	}

}
