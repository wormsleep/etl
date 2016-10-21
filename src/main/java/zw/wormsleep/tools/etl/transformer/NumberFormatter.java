package zw.wormsleep.tools.etl.transformer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

/**
 * Created by wormsleep on 2015/12/2.
 */
public class NumberFormatter implements Formatter {
    Logger logger = LoggerFactory.getLogger(NumberFormatter.class);

    private String length;
    private String precision;

    public NumberFormatter(String length, String precision) {
        this.length = length;
        this.precision = precision;
    }

    @Override
    public Object format(Object value) {
        Object result = null;

        if (value != null && !value.equals("")) {
            String vs = pretreat(String.valueOf(value));

            BigDecimal d = null;
            try {
                d = new BigDecimal(vs);
            } catch (Exception e) {
                logger.debug("@@@ 无法转换 {} 为合法数值( N[{},{}] )！", value, length, precision);
            }
            if (d != null) {
                result = String.format("%." + precision + "f", d);
            }

        }

        return result != null ? result : "0";
    }

    // 使（数）值字符串有效化的预处理
    private String pretreat(String value) {
        return value.trim().replaceAll("\\.\\.", ".");
    }

}
