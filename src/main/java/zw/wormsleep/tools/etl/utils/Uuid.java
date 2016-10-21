package zw.wormsleep.tools.etl.utils;

import java.util.UUID;

/**
 * Created by xingjian on 15/5/29.
 */
public class Uuid {
    public static synchronized String getUuid() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    public static boolean checkValid(String uuid) {
        return (uuid != null && !uuid.equals("") && uuid.length() == 32) ? true : false;
    }
}
