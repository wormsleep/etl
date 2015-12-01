package zw.wormsleep.tools.etl.utils;

import java.util.List;

/**
 * Created by wormsleep on 2015/12/1.
 */
public class SQLUtils {

    public static class Transformer{
        public static String getSelectWithAliasLowerCaseColumnName(List<String> columnsName, String tableName) {
            StringBuffer select = new StringBuffer();

            select.append("select ");

            for(String columnName : columnsName) {
                select.append(columnName + " as " + columnName.toLowerCase() + ",");
            }

            return select.toString().substring(0, select.length()-1) + " from " + tableName;
        }
    }
}
