package zw.wormsleep.tools.etl.transformer;


import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.Date;

/**
 * Created by wormsleep on 2015/12/1.
 */
public class DateFormatter implements Formatter {
    public final String[] PARSEPATTERNS = new String[]{"yyyy-MM",
            "yyyyMM", "yyyy/MM", "yyyyMMdd", "yyyy-MM-dd", "yyyy/MM/dd",
            "yyyyMMddHHmmss", "yyyy-MM-dd HH:mm:ss", "yyyy/MM/dd HH:mm:ss"};
    Logger logger = LoggerFactory.getLogger(DateFormatter.class);

    @Override
    public Object format(Object value) {

        Object result = null;

        if (value instanceof java.util.Date || value instanceof java.sql.Date) {
            result = value;
        } else {
            if (value != null && value instanceof String && !value.equals("")) {
                Date date = null;
                try {
                    date = DateUtils.parseDate(String.valueOf(value), PARSEPATTERNS);
                } catch (ParseException e) {
                    logger.debug("@@@ 非法日期！以 NULL 处理。");
                }
                if (date != null) {
                    result = new java.sql.Date(date.getTime());
                }
            }
        }

        return result;

    }
}
