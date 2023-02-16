package mall.streamer.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.util.List;

/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2022/10/08/11:47
 * @Description:  After all the data is in Kafka, while dimensional data is stored in Hbase and others are segmented and remain in Kafka.
 */
public class DIMUtil {
    public static JSONObject getDimFromPhoenix(Connection phoenixConn, String tableName, Long id) {

        String sql = "select * from " + tableName + " where id=?";

        String[] args = {id.toString()};

        List<JSONObject> list = JdbcUtil.queryList(phoenixConn, sql, args, JSONObject.class);

        return list.size() == 0 ? null : list.get(0);
    }

    //
    private static JSONObject getDimFromRedis(Jedis redisClient, String tableName, Long id) {
        String key = tableName + ":" + id;

        String dimString = redisClient.get(key);

        if (dimString != null) {
            // 更新过期时间
            redisClient.expire(key, 2 * 24 * 60 * 60);
            return JSON.parseObject(dimString);
        }

        return null;
    }

    // using redis as buffers to avoid too much pressure on one single spot.
    private static void writeDimToRedis(Jedis redisClient, String tableName, Long id, JSONObject dim) {
        String key = tableName + ":" + id;
        String value = dim.toJSONString();

        /*redisClient.set(key, value);
        redisClient.expire(key, 2 * 24 * 60 * 60);
        endurance time.
        */

        redisClient.setex(key, 2 * 24 * 60 * 60, value);
    }

    public static JSONObject getDim(Jedis redisClient,
                                    Connection phoenixConn,
                                    String tableName,
                                    Long id) {

        // 1. if there is already in the buffer
        JSONObject dim = getDimFromRedis(redisClient, tableName, id);
        if (dim == null) {
            System.out.println(tableName + "  " + id + " hbase");
            // 2. if not, read from HbasePhoenix
            dim = getDimFromPhoenix(phoenixConn, tableName, id);
            // 3.write to redis
            writeDimToRedis(redisClient, tableName, id, dim);
        }else{
            System.out.println(tableName + "  " + id + " buffer");
        }
        return dim;
    }
}
