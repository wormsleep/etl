package zw.wormsleep.tools.etl.config;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zw.wormsleep.tools.etl.utils.ConfigParserUtils;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SimpleExtractConfig implements ExtractConfig {
    final Logger logger = LoggerFactory.getLogger(SimpleExtractConfig.class);

    final String NODE_INPUT = "input";
    final String PROP_INPUT_TYPE = "input[@type]";
    final String NODE_INPUT_FILETYPE = "input.filetype";
    final String NODE_INPUT_SEPARATOR = "input.separator";
    final String NODE_INPUT_ENCODING = "input.encoding";
    final String NODE_INPUT_DATABASE = "input.database";
    final String PROP_INPUT_DATABASE_TYPE = "input.database[@type]";
    final String NODE_INPUT_SQL = "input.sql";
    final String NODE_INPUT_TABLE = "input.table";
    final String NODE_INPUT_XPATH = "input.xpath";
    final String NODE_INPUT_ROW_START = "input.rowstart";
    final String NODE_INPUT_ROW_END = "input.rowend";
    final String NODE_INPUT_MAX_HEADER_ROWS = "input.maxheaderrows";
    final String NODE_INPUT_CATALOG = "input.catalog";
    final String NODE_INPUT_SCHEMA_PATTERN = "input.schemapattern";
    final String NODE_INPUT_TABLE_PATTERN = "input.tablepattern";
    final String NODE_INPUT_FETCH_SIZE = "input.fetchsize";
    final String NODE_INPUT_COLUMN_NAME_TOLOWERCASE = "input.columnnametolowercase";

    final String PROP_COLUMN_HEADER = "columns[@header]";
    final String NODE_COLUMN = "columns.column";
    final String NODE_INDEX = "index";
    final String NODE_NAME = "name";
    final String NODE_FIELD = "field";
    final String PROP_CHECK = "[@check]";

    private HierarchicalConfiguration business;
    private Map<String, String> database;

    public SimpleExtractConfig(String businessType) throws ConfigurationException {
        business = ConfigParserUtils.getResourceConfiguration(businessType);
        database = ConfigParserUtils.getDatabaseConfiguration(business.getString(NODE_INPUT_DATABASE));
    }

    public SimpleExtractConfig(String businessType, File configuration) throws ConfigurationException {
        business = ConfigParserUtils.getResourceConfiguration(businessType, configuration);
        database = ConfigParserUtils.getDatabaseConfiguration(business.getString(NODE_INPUT_DATABASE), configuration);
    }

    @Override
    public Map<String, String> getCheckColumns() {
        Map<String, String> checkColums = new HashMap<String, String>();

        String name, field;
        List<HierarchicalConfiguration> columns = business
                .configurationsAt(NODE_COLUMN);
        for (HierarchicalConfiguration column : columns) {
            if (column.getString(PROP_CHECK) == null
                    || column.getString(PROP_CHECK).equalsIgnoreCase("true")) {
                name = column.getString(NODE_NAME);
                field = column.getString(NODE_FIELD);

                checkColums.put(name, field);
                logger.debug("@@@ Check Column : {} {}", name, field);
            }
        }

        return checkColums;
    }

    @Override
    public String getSeparator() {
        return ConfigParserUtils.getSeparator(business.getString(NODE_INPUT_SEPARATOR));
    }

    @Override
    public int getRowStart() {
        return business.getInt(NODE_INPUT_ROW_START, 0);
    }

    @Override
    public int getRowEnd() {
        return business.getInt(NODE_INPUT_ROW_END, 0);
    }

    @Override
    public int getMaxHeaderCheckRows() {
        return business.getInt(NODE_INPUT_MAX_HEADER_ROWS, 10);
    }

    @Override
    public Map<String, Integer> getIndexedColumns() {
        Map<String, Integer> indexedColumns = new HashMap<String, Integer>();

        List<HierarchicalConfiguration> columns = business
                .configurationsAt(NODE_COLUMN);

        int i = 0;
        for (HierarchicalConfiguration column : columns) {
            if (column.getString(PROP_CHECK) == null
                    || column.getString(PROP_CHECK).equalsIgnoreCase("true")) {

                String field = column.getString(NODE_FIELD);

                int index = column.getInt(NODE_INDEX, -1);
                int fieldIndex = index < 0 ? i++ : index;
                indexedColumns.put(field, fieldIndex);

                logger.debug("@@@ Indexed Column : {} {}", fieldIndex, field);
            }
        }

        return indexedColumns;
    }

    @Override
    public boolean hasHeader() {
        return business.getBoolean(PROP_COLUMN_HEADER, false);
    }

    @Override
    public String getEncoding() {
        return business.getString(NODE_INPUT_ENCODING, "UTF-8");
    }

    @Override
    public String getFileType() {
        return business.getString(NODE_INPUT_FILETYPE);
    }

    @Override
    public String getInputType() {
        return business.getString(PROP_INPUT_TYPE, "file");
    }

    @Override
    public String getDatabase() {
        return business.getString(NODE_INPUT_DATABASE);
    }

    @Override
    public String getSQL() {
        String sql = null;
        Object obj = business.getProperty(NODE_INPUT_SQL);

        if (obj != null) {
            sql = obj.toString();
            if (sql.startsWith("[") && sql.endsWith("]")) {
                sql = sql.substring(1, sql.length() - 1);
            }
        }
        return sql;
    }

    @Override
    public String getTable() {
        return business.getString(NODE_INPUT_TABLE);
    }

    @Override
    public Map<String, String> getDatabaseConfiguration() {
        return database;
    }

    @Override
    public String getXpath() {
        return business.getString(NODE_INPUT_XPATH);
    }

    @Override
    public String getCatalog() {
        String catalog = business.getString(NODE_INPUT_CATALOG);
        return (catalog != null && !catalog.equals("")) ? catalog : null;
    }

    @Override
    public String getSchemaPattern() {
        String schemaPattern = business.getString(NODE_INPUT_SCHEMA_PATTERN);
        return (schemaPattern != null && !schemaPattern.equals("")) ? schemaPattern : null;
    }

    @Override
    public String getTablePattern() {
        String tablePattern = business.getString(NODE_INPUT_TABLE_PATTERN);
        return (tablePattern != null && !tablePattern.equals("")) ? tablePattern : "%";
    }

    @Override
    public String getDatabaseType() {
        return business.getString(PROP_INPUT_DATABASE_TYPE);
    }

    @Override
    public int getFetchSize() {
        return business.getInt(NODE_INPUT_FETCH_SIZE, 0);
    }

    @Override
    public boolean columnNameToLowerCase() {
        return business.getBoolean(NODE_INPUT_COLUMN_NAME_TOLOWERCASE, false);
    }

}
