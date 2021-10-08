package mall.streamer.utils;

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
}
