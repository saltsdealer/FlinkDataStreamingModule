package mall.streamer.utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2021/10/08/15:49
 * @Description:
 */
public class ThreadPoolUtil {
    public static ThreadPoolExecutor getThreadPool(){
        return new ThreadPoolExecutor(
                100,
                300,
                30,
                TimeUnit.SECONDS,
                new LinkedBlockingDeque<>(50)
        );
    }
}
