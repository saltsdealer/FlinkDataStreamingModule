package mall.streamer.utils;

import lombok.SneakyThrows;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2021/09/28/16:32
 * @Description:
 */
public class IncUtil {
    public static <T> List<T> toList(Iterable<T> it){
        List<T> list = new ArrayList<>();
        for (T t : it) {
            list.add(t);
        }

        return list;
    }
    @SneakyThrows
    public static long toTs(String dateTime, String... format) {
        String f = "yyyy-MM-dd HH:mm:ss";
        if (format.length == 1) {
            f = format[0];
        }

        return new SimpleDateFormat(f).parse(dateTime).getTime();
    }

    public static String toDataTimeString(long ts) {

        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(ts);
    }
}
