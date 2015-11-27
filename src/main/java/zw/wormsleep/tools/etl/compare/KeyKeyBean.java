package zw.wormsleep.tools.etl.compare;

/**
 * Created by wormsleep on 2015/11/20.
 */
public class KeyKeyBean {
    private String separator;

    private String key1;
    private String key2;

    public KeyKeyBean(String key1, String key2, String separator) {
        this.key1 = key1;
        this.key2 = key2;
        this.separator = separator;
    }

    public String getKey1() {
        return key1;
    }

    public void setKey1(String key1) {
        this.key1 = key1;
    }

    public String getKey2() {
        return key2;
    }

    public void setKey2(String key2) {
        this.key2 = key2;
    }

    public boolean contains(KeyKeyBean another) {
        return (key1.equalsIgnoreCase(another.getKey1()) || key2.equalsIgnoreCase(another.getKey2())) ? true : false;
    }

    public boolean equals(KeyKeyBean another) {
        return (key1.equalsIgnoreCase(another.getKey1()) && key2.equalsIgnoreCase(another.getKey2())) ? true : false;
    }

    @Override
    public String toString() {
        return key1+separator+key2;
    }
}
