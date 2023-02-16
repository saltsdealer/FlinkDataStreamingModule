package mall.streamer.app.dwm;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import mall.streamer.bean.*;
import mall.streamer.common.Constants;

import mall.streamer.utils.*;
import mall.streamer.bean.AppModelV2;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.time.Duration;
import java.util.HashMap;

import static mall.streamer.common.Constants.TOPIC_DWD_ORDER_DETAIL;
import static mall.streamer.common.Constants.TOPIC_DWD_ORDER_INFO;

/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2022/09/28/16:39
 * @Description: 
 */
public class DwmOrderWideTable_Cache extends AppModelV2 {
    public static void main(String[] args) {
        new DwmOrderWideTable_Cache().init(
            3003,
            1,
            "DwmOrderWideTable",
            "DwmOrderWideTable",
            TOPIC_DWD_ORDER_INFO, TOPIC_DWD_ORDER_DETAIL);
    }
    
    @Override
    public void run(StreamExecutionEnvironment env,
                    HashMap<String, DataStreamSource<String>> streams) {
        // 1. 两个流进行join; 事实表join
        SingleOutputStreamOperator<OrderWideTable> OrderWideTableWithoutDimStream = joinFact(streams);
        // 2. 补充维度信息: join 维度
        SingleOutputStreamOperator<OrderWideTable> OrderWideTableWithDimStream = joinDim(OrderWideTableWithoutDimStream);
        OrderWideTableWithDimStream.print();
        // 3. 数据写入到kafka
        
    }
    
    private SingleOutputStreamOperator<OrderWideTable> joinDim(SingleOutputStreamOperator<OrderWideTable> stream) {
        return stream.map(new RichMapFunction<OrderWideTable, OrderWideTable>() {
            
            private Jedis redisClient;
            private Connection conn;
            
            @Override
            public void open(Configuration parameters) throws Exception {
                conn = JdbcUtil.getPhoenixConnection(Constants.PHOENIX_URL);
                
                redisClient = RedisUtil.getClient();
            }
            
            @Override
            public OrderWideTable map(OrderWideTable OrderWideTable) throws Exception {
                
                // 1. 补齐的用户维度
                JSONObject userInfo = DIMUtil.getDim(redisClient, conn, "dim_user_info", OrderWideTable.getUser_id());
                OrderWideTable.setUser_gender(userInfo.getString("GENDER"));
                OrderWideTable.calcUser_Age(userInfo.getString("BIRTHDAY"));
                
                // 2. 补齐省份信息
                JSONObject baseProvince = DIMUtil.getDim(redisClient, conn, "dim_base_province", OrderWideTable.getProvince_id());
                OrderWideTable.setProvince_name(baseProvince.getString("NAME"));
                OrderWideTable.setProvince_iso_code(baseProvince.getString("ISO_CODE"));
                OrderWideTable.setProvince_area_code(baseProvince.getString("AREA_CODE"));
                OrderWideTable.setProvince_3166_2_code(baseProvince.getString("ISO_3166_2"));
                
                // 3. 补齐skuInf
                JSONObject skuInfo = DIMUtil.getDim(redisClient, conn, "dim_sku_info", OrderWideTable.getSku_id());
                OrderWideTable.setSku_name(skuInfo.getString("SKU_NAME"));
                // 补齐spu_id tm_id c3_id
                
                OrderWideTable.setSpu_id(skuInfo.getLong("SPU_ID"));
                OrderWideTable.setTm_id(skuInfo.getLong("TM_ID"));
                OrderWideTable.setCategory3_id(skuInfo.getLong("CATEGORY3_ID"));
                
                // 4. spu
                JSONObject spuInfo = DIMUtil.getDim(redisClient, conn, "dim_spu_info", OrderWideTable.getSpu_id());
                OrderWideTable.setSpu_name(spuInfo.getString("SPU_NAME"));
                // 5. tm
                JSONObject tmInfo = DIMUtil.getDim(redisClient, conn, "dim_base_trademark", OrderWideTable.getTm_id());
                OrderWideTable.setTm_name(tmInfo.getString("TM_NAME"));
                
                // 6. c3
                JSONObject c3Info = DIMUtil.getDim(redisClient, conn, "dim_base_category3", OrderWideTable.getCategory3_id());
                OrderWideTable.setCategory3_name(c3Info.getString("NAME"));
                
                return OrderWideTable;
            }
            
            @Override
            public void close() throws Exception {
                if (conn != null) {
                    conn.close();
                }
    
                if (redisClient != null) {
                    redisClient.close();
                }
            }
        });
        
    }
    
    private SingleOutputStreamOperator<OrderWideTable> joinFact(HashMap<String, DataStreamSource<String>> streams) {
        KeyedStream<OrderInfo, Long> orderInfoStream = streams
            .get(TOPIC_DWD_ORDER_INFO)
            .map(data -> JSON.parseObject(data, OrderInfo.class))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((info, ts) -> info.getCreate_ts())
            )
            .keyBy(OrderInfo::getId);
        
        KeyedStream<OrderDetail, Long> orderDetailStream = streams
            .get(TOPIC_DWD_ORDER_DETAIL)
            .map(data -> JSON.parseObject(data, OrderDetail.class))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((detail, ts) -> detail.getCreate_ts())
            )
            .keyBy(OrderDetail::getOrder_id);
        
        return orderInfoStream
            .intervalJoin(orderDetailStream)
            .between(Time.seconds(-10), Time.seconds(10))
            .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWideTable>() {
                @Override
                public void processElement(OrderInfo left,
                                           OrderDetail right,
                                           Context ctx,
                                           Collector<OrderWideTable> out) throws Exception {
                    out.collect(new OrderWideTable(left, right));
                }
            });
    }
}
