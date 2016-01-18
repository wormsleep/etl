package zw.wormsleep.tools.etl.compare;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;

/**
 * Created by wormsleep on 2016/1/15.
 */
public class KeyKeySearchMemoryThread extends Thread {
    private Collection foundKeyKeys;
    private KeyKey[] sortedKeys;
    private KeyKey key;
    private Comparator<KeyKey> comparator;


    public KeyKeySearchMemoryThread(Collection<KeyKey> foundKeyKeys, KeyKey[] sortedKeys, KeyKey key, Comparator<KeyKey> comparator) {
        this.foundKeyKeys = foundKeyKeys;
        this.sortedKeys = sortedKeys;
        this.key = key;
        this.comparator = comparator;
    }

    @Override
    public void run() {
        String threadName = Thread.currentThread().getName();

        int position = Arrays.binarySearch(sortedKeys, key, comparator);
        // 若找到了再向左找和向右找
        if (position >= 0) {
            // 这是找到的任一条
            foundKeyKeys.add(sortedKeys[position]);
            // 基于该条数据的位置向左向右找
            int arraySize = sortedKeys.length;
            boolean leftRemaining = true;
            boolean rightRemaining = true;

            int leftPosition = position > 0 ? position - 1 : 0;
            int rightPosition = position < arraySize - 1 ? position + 1 : arraySize;
            do {

                if (leftRemaining) {
                    if (leftPosition < 1) {
                        leftRemaining = false;
                    } else {
                        if (comparator.compare(key, sortedKeys[leftPosition]) == 0) {
                            foundKeyKeys.add(sortedKeys[leftPosition--]);
                        } else {
                            leftRemaining = false;
                        }
                    }
                }

                if (rightRemaining) {
                    if (rightPosition < arraySize) {
                        if (comparator.compare(key, sortedKeys[rightPosition]) == 0) {
                            foundKeyKeys.add(sortedKeys[rightPosition++]);
                        } else {
                            rightRemaining = false;
                        }
                    } else {
                        rightRemaining = false;
                    }
                }

            } while (leftRemaining || rightRemaining);
        }
    }
}
