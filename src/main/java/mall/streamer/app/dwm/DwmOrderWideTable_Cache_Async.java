package mall.streamer.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import mall.streamer.bean.AppModelV2;
import mall.streamer.bean.OrderDetail;
import mall.streamer.bean.OrderInfo;
import mall.streamer.bean.OrderWideTable;
import mall.streamer.function.DimAsyncFunction;
import mall.streamer.utils.DIMUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.text.ParseException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static mall.streamer.common.Constants.TOPIC_DWD_ORDER_DETAIL;
import static mall.streamer.common.Constants.TOPIC_DWD_ORDER_INFO;

/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2021/09/28/16:39
 * @Description: WIP
 */
public class DwmOrderWideTable_Cache_Async extends AppModelV2 {
    public static void main(String[] args) {
        new DwmOrderWideTable_Cache_Async().init(
            3003,
            1,
            "DwmOrderWide_Cache_Async",
            "DwmOrderWide_Cache_Async",
            TOPIC_DWD_ORDER_INFO, TOPIC_DWD_ORDER_DETAIL);
    }
    
    @Override
    public void run(StreamExecutionEnvironment env,
                    HashMap<String, DataStreamSource<String>> streams) {

        SingleOutputStreamOperator<OrderWideTable> orderWideWithoutDimStream = joinFact(streams);

        SingleOutputStreamOperator<OrderWideTable> orderWideWithDimStream = joinDim(orderWideWithoutDimStream);
        orderWideWithDimStream.print();

        
    }
    
    private SingleOutputStreamOperator<OrderWideTable> joinDim(SingleOutputStreamOperator<OrderWideTable> stream) {
        return AsyncDataStream.unorderedWait(
            stream,
            new DimAsyncFunction<OrderWideTable>() {
                @Override
                public void addDim(Connection conn,
                                   Jedis redisClient,
                                   OrderWideTable orderWide,
                                   ResultFuture<OrderWideTable> resultFuture) {
                    // get the dimensional data
                    // 1. userinfo
                    JSONObject userInfo = DIMUtil.getDim(redisClient, conn, "dim_user_info", orderWide.getUser_id());
                    orderWide.setUser_gender(userInfo.getString("GENDER"));
                    try {
                        orderWide.calcUser_Age(userInfo.getString("BIRTHDAY"));
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                    
                    // 2. province
                    JSONObject baseProvince = DIMUtil.getDim(redisClient, conn, "dim_base_province", orderWide.getProvince_id());
                    orderWide.setProvince_name(baseProvince.getString("NAME"));
                    orderWide.setProvince_iso_code(baseProvince.getString("ISO_CODE"));
                    orderWide.setProvince_area_code(baseProvince.getString("AREA_CODE"));
                    orderWide.setProvince_3166_2_code(baseProvince.getString("ISO_3166_2"));
                    
                    // 3. skuInf
                    JSONObject skuInfo = DIMUtil.getDim(redisClient, conn, "dim_sku_info", orderWide.getSku_id());
                    orderWide.setSku_name(skuInfo.getString("SKU_NAME"));
                    
                    orderWide.setSpu_id(skuInfo.getLong("SPU_ID"));
                    orderWide.setTm_id(skuInfo.getLong("TM_ID"));
                    orderWide.setCategory3_id(skuInfo.getLong("CATEGORY3_ID"));
                    
                    // 4. spu
                    JSONObject spuInfo = DIMUtil.getDim(redisClient, conn, "dim_spu_info", orderWide.getSpu_id());
                    orderWide.setSpu_name(spuInfo.getString("SPU_NAME"));
                    // 5. tm
                    JSONObject tmInfo = DIMUtil.getDim(redisClient, conn, "dim_base_trademark", orderWide.getTm_id());
                    orderWide.setTm_name(tmInfo.getString("TM_NAME"));
                    
                    // 6. category3
                    JSONObject c3Info = DIMUtil.getDim(redisClient, conn, "dim_base_category3", orderWide.getCategory3_id());
                    orderWide.setCategory3_name(c3Info.getString("NAME"));
                    // returned data
                    resultFuture.complete(Collections.singletonList(orderWide));
                }
            },
            60,
            TimeUnit.SECONDS
        );
        
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
