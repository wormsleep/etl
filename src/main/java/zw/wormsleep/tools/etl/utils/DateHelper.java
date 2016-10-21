package zw.wormsleep.tools.etl.utils;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;

/**
 * 辅助类 - 处理格式化的日期
 *
 * @author zhaowei
 */
public class DateHelper {

    public static final String YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss";
    public static final String YYYYMM = "yyyyMM";

    /**
     * 年月遍历器
     *
     * @param start 起始年月
     * @param end   终止年月
     * @return
     * @throws ParseException
     */
    public static Iterator<String> monthIterator(final String start,
                                                 final String end) throws ParseException {

        return new Iterator<String>() {
            private Calendar calStart = toCalendar(start);
            private Calendar calEnd = toCalendar(end);

            @Override
            public boolean hasNext() {
                return (getYYYYMM(calStart).compareTo(getYYYYMM(calEnd)) <= 0 ? true
                        : false);
            }

            @Override
            public String next() {
                String result = getYYYYMM(calStart);
                calStart.add(Calendar.MONTH, 1);
                return result;
            }

            @Override
            public void remove() {
            }
        };

    }

    public static String getYearFirstDay() {
        return getYearFirstDay(Calendar.getInstance());
    }

    public static String getYearFirstDay(String str) throws ParseException {
        return getYearFirstDay(toCalendar(str));
    }

    public static String getYearFirstDay(Calendar calendar) {
        return getYearFirstDay(calendar, YYYY_MM_DD_HH_MM_SS);
    }

    public static String getYearFirstDay(Calendar calendar, String pattern) {
        calendar.set(Calendar.MONTH, 0);

        return getMonthFirstDay(calendar, pattern);
    }

    public static String getMonthFirstDay() {
        return getMonthFirstDay(Calendar.getInstance());
    }

    public static String getMonthFirstDay(String str) throws ParseException {
        return getMonthFirstDay(toCalendar(str));
    }

    public static String getMonthFirstDay(Calendar calendar) {
        return getMonthFirstDay(calendar, YYYY_MM_DD_HH_MM_SS);
    }

    public static String getMonthFirstDay(Calendar calendar, String pattern) {
        calendar.set(Calendar.DATE, 1);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);

        return format(calendar, pattern);
    }

    public static String getMonthLastDay() {
        return getMonthLastDay(Calendar.getInstance());
    }

    public static String getMonthLastDay(String str) throws ParseException {
        return getMonthLastDay(toCalendar(str));
    }

    public static String getMonthLastDay(Calendar calendar) {
        return getMonthLastDay(calendar, YYYY_MM_DD_HH_MM_SS);
    }

    public static String getMonthLastDay(Calendar calendar, String pattern) {
        calendar.set(Calendar.DATE, 1);
        calendar.add(Calendar.MONTH, 1);
        calendar.add(Calendar.DATE, -1);
        calendar.set(Calendar.HOUR_OF_DAY, 23);
        calendar.set(Calendar.MINUTE, 59);
        calendar.set(Calendar.SECOND, 59);

        return format(calendar, pattern);
    }

    public static String getLastMonthFirstDay(String str) throws ParseException {
        return getLastMonthFirstDay(toCalendar(str));
    }

    public static String getLastMonthFirstDay() {
        return getLastMonthFirstDay(Calendar.getInstance());
    }

    public static String getLastMonthFirstDay(Calendar calendar) {
        return getLastMonthFirstDay(calendar, YYYY_MM_DD_HH_MM_SS);
    }

    public static String getLastMonthFirstDay(Calendar calendar, String pattern) {
        if (calendar.get(Calendar.MONTH) > 0) {
            calendar.add(Calendar.MONTH, -1);
        }

        return getMonthFirstDay(calendar, pattern);
    }

    public static String getLastMonthLastDay(String str) throws ParseException {
        return getLastMonthLastDay(toCalendar(str));
    }

    public static String getLastMonthLastDay() {
        return getLastMonthLastDay(Calendar.getInstance());
    }

    public static String getLastMonthLastDay(Calendar calendar) {
        return getLastMonthLastDay(calendar, YYYY_MM_DD_HH_MM_SS);
    }

    public static String getLastMonthLastDay(Calendar calendar, String pattern) {
        if (calendar.get(Calendar.MONTH) > 0) {
            calendar.add(Calendar.MONTH, -1);
        }

        return getMonthLastDay(calendar, pattern);
    }

    public static String getYYYYMM() {
        return getYYYYMM(Calendar.getInstance());
    }

    public static String getYYYYMM(String str) throws ParseException {
        return getYYYYMM(toCalendar(str));
    }

    public static String getYYYYMM(Calendar calendar) {
        return format(calendar, YYYYMM);
    }

    public static String getYYYYMMHHMMSS() {
        return getYYYYMMHHMMSS(Calendar.getInstance());
    }

    public static String getYYYYMMHHMMSS(String str) throws ParseException {
        return getYYYYMMHHMMSS(toCalendar(str));
    }

    public static String getYYYYMMHHMMSS(Calendar calendar) {
        return format(calendar, YYYY_MM_DD_HH_MM_SS);
    }

    public static String format(Calendar calendar, String pattern) {
        return DateFormatUtils.format(calendar, pattern);
    }


    public static Calendar toCalendar(String str) throws ParseException {
        int len = str.length();
        Date date = null;

        if (len == 4) {
            date = DateUtils.parseDate(str, new String[]{"yyyy"});
        } else if (len == 6) {
            date = DateUtils.parseDate(str, new String[]{"yyyyMM"});
        } else if (len == 7) {
            date = DateUtils.parseDate(str, new String[]{"yyyy-MM", "yyyy/MM"});
        } else if (len == 8) {
            date = DateUtils.parseDate(str, new String[]{"yyyyMMdd"});
        } else if (len == 10) {
            date = DateUtils.parseDate(str, new String[]{"yyyy-MM-dd", "yyyy/MM/dd"});
        } else if (len == 19) {
            date = DateUtils.parseDate(str, new String[]{"yyyy-MM-dd HH:mm:ss", "yyyy/MM/dd HH:mm:ss"});
        } else {
            date = new Date();
        }

        return DateUtils.toCalendar(date);
    }
}