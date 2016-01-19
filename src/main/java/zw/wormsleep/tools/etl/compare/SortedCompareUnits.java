package zw.wormsleep.tools.etl.compare;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created by wormsleep on 2016/1/12.
 */
public class SortedCompareUnits {
    final Logger logger = LoggerFactory.getLogger(SortedCompareUnits.class);

    private File in;
    private String encoding;
    private String separator;

    private CompareUnit[] compareUnits;
    private Map<Integer, Integer[]> groupedLengthObject;
    private Integer[] groupedLength;

    public SortedCompareUnits(File in, String encoding, String separator) throws IOException {
        this.in = in;
        this.encoding = encoding;
        this.separator = separator;
        init();
    }

    private void init() throws IOException {
        List<String> lines = FileUtils.readLines(in, encoding);

        // 组装有效比对数据对象集
        List<CompareUnit> _compareUnits = new ArrayList<CompareUnit>();
        for (String line : lines) {
            String[] ls = line.split(separator);
            if (ls.length > 1) {
                _compareUnits.add(new CompareUnit(ls[0], ls[1], ls[1].length()));
            }
        }

        // toArray
        compareUnits = new CompareUnit[_compareUnits.size()];
        int index = 0;
        for (CompareUnit cu : _compareUnits) {
            compareUnits[index++] = cu;
        }

        Arrays.sort(compareUnits, new Comparator<CompareUnit>() {
            @Override
            public int compare(CompareUnit o1, CompareUnit o2) {
                return o1.length > o2.length ? 1 : (o1.length < o2.length ? -1 : 0);
            }
        });

        // 抽取分组长度集对象 - 结构 {长度, [起始位置, 终止位置]}
        groupedLengthObject = new LinkedHashMap<Integer, Integer[]>();
        Integer len;
        index = 0;
        for (CompareUnit cu : compareUnits) {
            len = cu.length;

            if (groupedLengthObject.containsKey(len)) {
                groupedLengthObject.put(len, new Integer[]{groupedLengthObject.get(len)[0], index});
            } else {
                groupedLengthObject.put(len, new Integer[]{index, index});
            }

            index++;
        }

        // 分组长度对象之长度数组
        groupedLength = new Integer[groupedLengthObject.size()];
        index = 0;
        for (Integer glen : groupedLengthObject.keySet()) {
            groupedLength[index++] = glen;
        }
    }

    /**
     * 获取比对对象子集
     *
     * @param length 待比对内容长度
     * @param scope  正负范围
     * @return
     */
    public CompareUnit[] getLimitedCompareUnits(int length, int scope) {
        int start = -1;
        int end = -1;

        // 优化 - 策略 - 若指定长度大于4认为是企业名称按范围提取数据，反之认为是人名仅提供匹配长度
        if(length > 4) {
            int upperLimit = (length - scope) > 4 ? (length - scope) : 4;
            int lowerLimit = length + scope;

            for (int i = 0; i < groupedLength.length; i++) {
                int glen = groupedLength[i];
                if (upperLimit <= glen) {
                    start = groupedLengthObject.get(glen)[0];
                    break;
                }
            }

            for (int i = groupedLength.length - 1; i >= 0; i--) {
                int glen = groupedLength[i];
                if (lowerLimit >= glen) {
                    end = groupedLengthObject.get(glen)[1];
                    break;
                }
            }

            return (end >= start && start >= 0 && end >= 0) ? Arrays.copyOfRange(compareUnits, start, end+1) : new CompareUnit[]{};

        } else {
            int position = Arrays.binarySearch(groupedLength, length);

            return position >= 0 ? Arrays.copyOfRange(compareUnits, groupedLengthObject.get(groupedLength[position])[0], groupedLengthObject.get(groupedLength[position])[1]+1) : new CompareUnit[]{};
        }
    }

    public CompareUnit[] getCompareUnits() {
        return compareUnits;
    }

    public Map<Integer, Integer[]> getGroupedLengthObject() {
        return groupedLengthObject;
    }
}
