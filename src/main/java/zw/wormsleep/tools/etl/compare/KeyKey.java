package zw.wormsleep.tools.etl.compare;

import java.util.Comparator;

/**
 * Created by wormsleep on 2015/11/20.
 */
public class KeyKey {
    private String separator;
    private String key1;
    private String key2;

    private int index = -1;
    private boolean grouped = false;

    public KeyKey(String key1, String key2, String separator) {
        this.key1 = key1;
        this.key2 = key2;
        this.separator = separator;
    }

    public String getSeparator() {
        return separator;
    }

    public void setSeparator(String separator) {
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

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public boolean isGrouped() {
        return grouped;
    }

    public void setGrouped(boolean grouped) {
        this.grouped = grouped;
    }

    public boolean contains(KeyKey another) {
        return (key1.equals(another.getKey1()) || key2.equals(another.getKey2())) ? true : false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        KeyKey keyKey = (KeyKey) o;

        if (!key1.equals(keyKey.key1)) return false;
        return key2.equals(keyKey.key2);

    }

    @Override
    public int hashCode() {
        int result = key1.hashCode();
        result = 31 * result + key2.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return key1 + separator + key2;
    }

    public String description() {
        return String.format(" KeyKey - key1: %s || key2: %s || index: %s || grouped: %s ",
                key1, key2, String.valueOf(index), String.valueOf(grouped));
    }

    public static Comparator<KeyKey> key1Comparator() {
        return new Comparator<KeyKey>() {
            @Override
            public int compare(KeyKey o1, KeyKey o2) {
                return o1.getKey1().compareTo(o2.getKey1());
            }
        };
    }

    public static Comparator<KeyKey> key2Comparator() {
        return new Comparator<KeyKey>() {
            @Override
            public int compare(KeyKey o1, KeyKey o2) {
                return o1.getKey2().compareTo(o2.getKey2());
            }
        };
    }
}
