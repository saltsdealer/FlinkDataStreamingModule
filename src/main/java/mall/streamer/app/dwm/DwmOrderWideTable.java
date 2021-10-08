package mall.streamer.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import mall.streamer.bean.OrderDetail;
import mall.streamer.bean.OrderInfo;
import mall.streamer.bean.OrderWideTable;
import mall.streamer.common.Constants;
import mall.streamer.utils.DIMUtil;
import mall.streamer.bean.AppModelV2;
import mall.streamer.utils.JdbcUtil;
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

import java.sql.Connection;
import java.time.Duration;
import java.util.HashMap;

import static mall.streamer.common.Constants.TOPIC_DWD_ORDER_DETAIL;
import static mall.streamer.common.Constants.TOPIC_DWD_ORDER_INFO;


/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2021/09/28/16:39
 * @Description:
 */
public class DwmOrderWideTable extends AppModelV2 {
    public static void main(String[] args) {
        new DwmOrderWideTable().init(
            3003,
            1,
            "DwmOrderWide",
            "DwmOrderWide",
            TOPIC_DWD_ORDER_INFO, TOPIC_DWD_ORDER_DETAIL);
    }
    
    @Override
    public void run(StreamExecutionEnvironment env,
                    HashMap<String, DataStreamSource<String>> streams) {
        // 1. 两个流进行join; 事实表join
        SingleOutputStreamOperator<OrderWideTable> orderWideWithoutDimStream = joinFact(streams);
        // 2. 补充维度信息: join 维度
        SingleOutputStreamOperator<OrderWideTable> orderWideWithDimStream = joinDim(orderWideWithoutDimStream);
        orderWideWithDimStream.print();
        // 3. 数据写入到kafka
        
    }
    
    private SingleOutputStreamOperator<OrderWideTable> joinDim(SingleOutputStreamOperator<OrderWideTable> stream) {
        return stream.map(new RichMapFunction<OrderWideTable, OrderWideTable>() {
            
            private Connection conn;
            
            @Override
            public void open(Configuration parameters) throws Exception {
                conn = JdbcUtil.getPhoenixConnection(Constants.PHOENIX_URL);
            }
            
            @Override
            public OrderWideTable map(OrderWideTable orderWide) throws Exception {
                
                // 1. 补齐的用户维度
                JSONObject userInfo = DIMUtil.getDimFromPhoenix(conn, "dim_user_info", orderWide.getUser_id());
                orderWide.setUser_gender(userInfo.getString("GENDER"));
                orderWide.calcUser_Age(userInfo.getString("BIRTHDAY"));
                
                // 2. 补齐省份信息
                JSONObject baseProvince = DIMUtil.getDimFromPhoenix(conn, "dim_base_province", orderWide.getProvince_id());
                orderWide.setProvince_name(baseProvince.getString("NAME"));
                orderWide.setProvince_iso_code(baseProvince.getString("ISO_CODE"));
                orderWide.setProvince_area_code(baseProvince.getString("AREA_CODE"));
                orderWide.setProvince_3166_2_code(baseProvince.getString("ISO_3166_2"));
                
                // 3. 补齐skuInf
                JSONObject skuInfo = DIMUtil.getDimFromPhoenix(conn, "dim_sku_info", orderWide.getSku_id());
                orderWide.setSku_name(skuInfo.getString("SKU_NAME"));
                // 补齐spu_id tm_id c3_id
                
                orderWide.setSpu_id(skuInfo.getLong("SPU_ID"));
                orderWide.setTm_id(skuInfo.getLong("TM_ID"));
                orderWide.setCategory3_id(skuInfo.getLong("CATEGORY3_ID"));
                
                // 4. spu
                JSONObject spuInfo = DIMUtil.getDimFromPhoenix(conn, "dim_spu_info", orderWide.getSpu_id());
                orderWide.setSpu_name(spuInfo.getString("SPU_NAME"));
                // 5. tm
                JSONObject tmInfo = DIMUtil.getDimFromPhoenix(conn, "dim_base_trademark", orderWide.getTm_id());
                orderWide.setTm_name(tmInfo.getString("TM_NAME"));
                
                // 6. c3
                JSONObject c3Info = DIMUtil.getDimFromPhoenix(conn, "dim_base_category3", orderWide.getCategory3_id());
                orderWide.setCategory3_name(c3Info.getString("NAME"));
                
                return orderWide;
            }
            
            @Override
            public void close() throws Exception {
                if (conn != null) {
                    conn.close();
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
