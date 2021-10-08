package mall.streamer.function;

import mall.streamer.common.Constants;
import mall.streamer.utils.JdbcUtil;
import mall.streamer.utils.RedisUtil;
import mall.streamer.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2021/10/08/16:24
 * @Description:
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> {

    private ThreadPoolExecutor threadPool;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 获取线程池
        threadPool = ThreadPoolUtil.getThreadPool();
    }

    public abstract void addDim(Connection phoenixConn,
                                Jedis client,
                                T input,
                                ResultFuture<T> resultFuture);

    @Override
    public void asyncInvoke(T input,
                            ResultFuture<T> resultFuture) throws Exception {
        // 多线程的方式
        threadPool.submit(new Runnable() {
            @Override
            public void run() {
                // 实现读取维度信息的代码
                Connection phoenixConn = JdbcUtil.getPhoenixConnection(Constants.PHOENIX_URL);
                Jedis client = RedisUtil.getClient();

                addDim(phoenixConn, client, input, resultFuture);

                try {
                    phoenixConn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                client.close(); // 关闭redis的客户端?  如果客户端是通过连接池获取的, 则是归还给连接池. 如果客户端是通过new Jedis创建的, 则是关闭客户端

            }
        });
    }


}
