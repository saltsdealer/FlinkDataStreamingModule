package mall.streamer.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2021/10/08/11:46
 * @Description: Redis is used for buffers.
 */
public class RedisUtil {

    private static JedisPool pool;



    static {
        JedisPoolConfig conf = new JedisPoolConfig();
        conf.setMaxTotal(300);
        conf.setMaxIdle(100);
        conf.setMinIdle(2);
        conf.setTestOnCreate(true);
        conf.setTestOnBorrow(true);
        conf.setTestOnReturn(true);
        conf.setMaxWaitMillis(10000);
        pool = new JedisPool(conf, "hadoop102", 6379);
    }

    public static Jedis getClient() {
        Jedis client = pool.getResource();
        client.select(1);
        return client;
    }
}
