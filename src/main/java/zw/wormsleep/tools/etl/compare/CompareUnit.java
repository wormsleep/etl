package zw.wormsleep.tools.etl.compare;

/**
 * Created by wormsleep on 2016/1/12.
 * 比对单元 - 描述每个比对记录的要素，主要作为二分排序用
 */
public class CompareUnit {
    public String key; // 比对内容主键
    public String content; // 比对内容
    public int length; // 比对内容长度

    public CompareUnit(String key, String content, int length) {
        this.key = key;
        this.content = content;
        this.length = length;
    }

    @Override
    public String toString() {
        return "CompareUnit: {" +
                "key='" + key + '\'' +
                ", content='" + content + '\'' +
                ", length=" + length +
                '}';
    }
}
