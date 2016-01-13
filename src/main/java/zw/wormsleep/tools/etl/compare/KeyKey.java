package zw.wormsleep.tools.etl.compare;

/**
 * Created by wormsleep on 2015/11/20.
 */
public class KeyKey {
    private String separator;

    private String key1;
    private String key2;

    public KeyKey(String key1, String key2, String separator) {
        this.key1 = key1;
        this.key2 = key2;
        this.separator = separator;
    }

    public String getKey1() {
        return key1;
    }

    public String getKey2() {
        return key2;
    }

    public boolean contains(KeyKey another) {
        return (key1.equals(another.getKey1()) || key2.equals(another.getKey2())) ? true : false;
    }

    public boolean equals(KeyKey another) {
        return (key1.equals(another.getKey1()) && key2.equals(another.getKey2())) ? true : false;
    }

    @Override
    public String toString() {
        return key1 + separator + key2;
    }
}
