package mall.streamer.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import mall.streamer.bean.*;
import mall.streamer.function.DimAsyncFunction;
import mall.streamer.utils.DIMUtil;
import mall.streamer.utils.FlinkSinkUtil;
import mall.streamer.utils.IncUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static mall.streamer.common.Constants.*;

/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2021/10/11/15:38
 * @Description:
 */
public class DwsProductStatsApp extends AppModelV2 {
    public static void main(String[] args) {
        new DwsProductStatsApp().init(
                4001,
                1,
                "DwsProductStatsApp",
                "DwsProductStatsApp",
                TOPIC_DWD_PAGE, TOPIC_DWD_DISPLAY,
                TOPIC_DWD_FAVOR_INFO, TOPIC_DWD_CART_INFO,
                TOPIC_DWM_ORDER_WIDE, TOPIC_DWM_PAYMENT_WIDE,
                TOPIC_DWD_REFUND_PAYMENT, TOPIC_DWD_COMMENT_INFO

        );

    }


    @Override
    public void run(StreamExecutionEnvironment env,
                    HashMap<String, DataStreamSource<String>> streams) {

        // 1. union to one stream
        DataStream<ProductStats> psStream = parseAndUnionOne(streams);
        // 2. window and aggregate
        SingleOutputStreamOperator<ProductStats> aggregatedStream = aggregate(psStream);
        // 3. join dimensions
        SingleOutputStreamOperator<ProductStats> psStreamWithDim = joinDim(aggregatedStream);
        // 4. write to clickhouse
        writeToClickHouse(psStreamWithDim);


    }


    private void writeToClickHouse(SingleOutputStreamOperator<ProductStats> psStreamWithDim) {
        psStreamWithDim.addSink(FlinkSinkUtil.getClickHouseSink(
                "gmall2021", "product_stats_2021", ProductStats.class
        ));
    }

    private SingleOutputStreamOperator<ProductStats> joinDim(SingleOutputStreamOperator<ProductStats> stream) {
        return AsyncDataStream.unorderedWait(
                stream,
                new DimAsyncFunction<ProductStats>() {
                    @Override
                    public void addDim(Connection conn,
                                       Jedis redisClient,
                                       ProductStats ps,
                                       ResultFuture<ProductStats> resultFuture) {
                        // 3. skuInf
                        JSONObject skuInfo = DIMUtil.getDim(redisClient, conn, "dim_sku_info", ps.getSku_id());
                        ps.setSku_name(skuInfo.getString("SKU_NAME"));
                        // spu_id tm_id c3_id

                        ps.setSku_price(skuInfo.getBigDecimal("PRICE"));

                        ps.setSpu_id(skuInfo.getLong("SPU_ID"));
                        ps.setTm_id(skuInfo.getLong("TM_ID"));
                        ps.setCategory3_id(skuInfo.getLong("CATEGORY3_ID"));

                        // 4. spu
                        JSONObject spuInfo = DIMUtil.getDim(redisClient, conn, "dim_spu_info", ps.getSpu_id());
                        ps.setSpu_name(spuInfo.getString("SPU_NAME"));
                        // 5. tm
                        JSONObject tmInfo = DIMUtil.getDim(redisClient, conn, "dim_base_trademark", ps.getTm_id());
                        ps.setTm_name(tmInfo.getString("TM_NAME"));

                        // 6. c3
                        JSONObject c3Info = DIMUtil.getDim(redisClient, conn, "dim_base_category3", ps.getCategory3_id());
                        ps.setCategory3_name(c3Info.getString("NAME"));
                        // 返回的数据放入的resultFuture中
                        resultFuture.complete(Collections.singletonList(ps));
                    }
                },
                60,
                TimeUnit.SECONDS
        );
    }

    private SingleOutputStreamOperator<ProductStats> aggregate(DataStream<ProductStats> stream) {
        return stream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((ps, ts) -> ps.getTs())
                )
                .keyBy(ProductStats::getSku_id)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(
                        new ReduceFunction<ProductStats>() {
                            @Override
                            public ProductStats reduce(ProductStats ps1,
                                                       ProductStats ps2) throws Exception {
                                ps1.setClick_ct(ps1.getClick_ct() + ps2.getClick_ct());
                                ps1.setDisplay_ct(ps1.getDisplay_ct() + ps2.getDisplay_ct());
                                ps1.setFavor_ct(ps1.getFavor_ct() + ps2.getFavor_ct());
                                ps1.setCart_ct(ps1.getCart_ct() + ps2.getCart_ct());

                                ps1.setOrder_amount(ps1.getOrder_amount().add(ps2.getOrder_amount()));
                                ps1.setPayment_amount(ps1.getPayment_amount().add(ps2.getPayment_amount()));
                                ps1.setRefund_amount(ps1.getRefund_amount().add(ps2.getRefund_amount()));

                                ps1.setOrder_sku_num(ps1.getOrder_sku_num() + ps2.getOrder_sku_num());
                                ps1.setComment_ct(ps1.getComment_ct() + ps2.getComment_ct());
                                ps1.setGood_comment_ct(ps1.getGood_comment_ct() + ps2.getGood_comment_ct());

                                ps1.getOrderIdSet().addAll(ps2.getOrderIdSet());
                                ps1.getRefundOrderIdSet().addAll(ps2.getRefundOrderIdSet());
                                ps1.getPaidOrderIdSet().addAll(ps2.getPaidOrderIdSet());

                                return ps1;
                            }
                        },
                        new ProcessWindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                            @Override
                            public void process(Long skuId,
                                                Context ctx,
                                                Iterable<ProductStats> elements,
                                                Collector<ProductStats> out) throws Exception {
                                ProductStats ps = elements.iterator().next();

                                TimeWindow w = ctx.window();

                                ps.setStt(IncUtil.toDataTimeString(w.getStart()));
                                ps.setEdt(IncUtil.toDataTimeString(w.getEnd()));

                                ps.setTs(System.currentTimeMillis());

                                ps.setOrder_ct((long) ps.getOrderIdSet().size());
                                ps.setRefund_order_ct((long) ps.getRefundOrderIdSet().size());
                                ps.setPaid_order_ct((long) ps.getPaidOrderIdSet().size());

                                out.collect(ps);
                            }
                        }
                );

    }

    private DataStream<ProductStats> parseAndUnionOne(HashMap<String, DataStreamSource<String>> streams) {
        // 1. clicks
        SingleOutputStreamOperator<ProductStats> clickStream = streams
                .get(TOPIC_DWD_PAGE)
                .flatMap(new FlatMapFunction<String, ProductStats>() {
                    @Override
                    public void flatMap(String value,
                                        Collector<ProductStats> out) throws Exception {
                        JSONObject obj = JSON.parseObject(value);
                        JSONObject page = obj.getJSONObject("page");

                        String pageId = page.getString("page_id");
                        String itemType = page.getString("item_type");

                        if ("good_detail".equals(pageId) && "sku_id".equals(itemType)) {
                            Long skuId = page.getLong("item");

                            ProductStats ps = new ProductStats();
                            ps.setSku_id(skuId);
                            ps.setClick_ct(1L);
                            ps.setTs(obj.getLong("ts"));

                            out.collect(ps);
                        }
                    }
                });

        // 2.exposure
        SingleOutputStreamOperator<ProductStats> displayStream = streams
                .get(TOPIC_DWD_DISPLAY)
                .filter(value -> {
                    JSONObject obj = JSON.parseObject(value);
                    String item = obj.getString("item");

                    return item.matches("^\\d+$");
                /*try {
                    Long.parseLong(item);
                }catch (Exception e){
                    return false;
                }
                return true;*/

                })
                .map(value -> {
                    JSONObject obj = JSON.parseObject(value);
                    ProductStats ps = new ProductStats();
                    ps.setSku_id(obj.getLong("item"));
                    ps.setDisplay_ct(1L);
                    ps.setTs(obj.getLong("ts"));

                    return ps;
                });

        // 3. favor
        SingleOutputStreamOperator<ProductStats> favorStream = streams
                .get(TOPIC_DWD_FAVOR_INFO)
                .map(value -> {
                    JSONObject obj = JSON.parseObject(value);
                    Long skuId = obj.getLong("sku_id");
                    long ts = IncUtil.toTs(obj.getString("create_time"));

                    ProductStats ps = new ProductStats();
                    ps.setSku_id(skuId);
                    ps.setTs(ts);
                    ps.setFavor_ct(1L);
                    return ps;

                });

        // 4. cart
        SingleOutputStreamOperator<ProductStats> cartStream = streams
                .get(TOPIC_DWD_CART_INFO)
                .map(value -> {
                    JSONObject obj = JSON.parseObject(value);
                    Long skuId = obj.getLong("sku_id");
                    long ts = IncUtil.toTs(obj.getString("create_time"));

                    ProductStats ps = new ProductStats();
                    ps.setSku_id(skuId);
                    ps.setTs(ts);
                    ps.setCart_ct(1L);
                    return ps;

                });
        // 5. ordered
        SingleOutputStreamOperator<ProductStats> orderStream = streams
                .get(TOPIC_DWM_ORDER_WIDE)
                .map(value -> {
                    OrderWideTable orderWide = JSON.parseObject(value, OrderWideTable.class);

                    ProductStats ps = new ProductStats();
                    ps.setSku_id(orderWide.getSku_id());
                    ps.getOrderIdSet().add(orderWide.getOrder_id());

                    ps.setOrder_amount(orderWide.getSplit_total_amount());
                    ps.setOrder_sku_num(orderWide.getSku_num());

                    ps.setTs(IncUtil.toTs(orderWide.getCreate_time()));

                    return ps;

                });

        // 6.payment
        SingleOutputStreamOperator<ProductStats> paymentStream = streams
                .get(TOPIC_DWM_PAYMENT_WIDE)
                .map(value -> {
                    PaymentWideTable pw = JSON.parseObject(value, PaymentWideTable.class);

                    ProductStats ps = new ProductStats();
                    ps.setSku_id(pw.getSku_id());
                    ps.getPaidOrderIdSet().add(pw.getOrder_id());

                    ps.setPayment_amount(pw.getSplit_total_amount());

                    ps.setTs(IncUtil.toTs(pw.getPayment_create_time()));

                    return ps;
                });

        // 7. refund
        SingleOutputStreamOperator<ProductStats> refundStream = streams
                .get(TOPIC_DWD_REFUND_PAYMENT)
                .map(value -> {
                    JSONObject refund = JSON.parseObject(value);

                    ProductStats ps = new ProductStats();
                    ps.setSku_id(refund.getLong("sku_id"));

                    ps.getRefundOrderIdSet().add(refund.getLong("order_id"));

                    ps.setRefund_amount(refund.getBigDecimal("total_amount"));

                    ps.setTs(IncUtil.toTs(refund.getString("create_time")));

                    return ps;
                });

        // 8. comments
        SingleOutputStreamOperator<ProductStats> commentStream = streams
                .get(TOPIC_DWD_COMMENT_INFO)
                .map(value -> {
                    JSONObject refund = JSON.parseObject(value);

                    ProductStats ps = new ProductStats();
                    ps.setSku_id(refund.getLong("sku_id"));

                    ps.setTs(IncUtil.toTs(refund.getString("create_time")));

                    ps.setComment_ct(1L);

                    String appraise = refund.getString("appraise");
                    if (FIVE_STAR.equals(appraise) || FOUR_STAR.equals(appraise)) {
                        ps.setGood_comment_ct(1L);

                    }
                    return ps;
                });

        return clickStream.union(
                displayStream,
                favorStream,
                cartStream,
                orderStream,
                paymentStream,
                refundStream,
                commentStream);
    }


}
