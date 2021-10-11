package mall.streamer.app.dwm;

import com.alibaba.fastjson.JSON;
import mall.streamer.bean.AppModelV2;
import mall.streamer.bean.OrderWideTable;
import mall.streamer.bean.PaymentInfo;
import mall.streamer.bean.PaymentWideTable;
import mall.streamer.common.Constants;
import mall.streamer.utils.FlinkSinkUtil;
import mall.streamer.utils.IncUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashMap;

import static mall.streamer.common.Constants.TOPIC_DWD_PAYMENT_INFO;
import static mall.streamer.common.Constants.TOPIC_DWM_ORDER_WIDE;

/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2021/10/11/15:33
 * @Description:
 */
public class DwmPaymentWideTable extends AppModelV2 {
    public static void main(String[] args) {
        new DwmPaymentWideTable().init(
                3004,
                1,
                "DwmPaymentWide",
                "DwmPaymentWide",
                TOPIC_DWM_ORDER_WIDE, TOPIC_DWD_PAYMENT_INFO);
    }

    @Override
    public void run(StreamExecutionEnvironment env,
                    HashMap<String, DataStreamSource<String>> streams) {
        KeyedStream<PaymentInfo, Long> paymentInfoStream = streams
                .get(TOPIC_DWD_PAYMENT_INFO)
                .map(info -> JSON.parseObject(info, PaymentInfo.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((info, ts) -> IncUtil.toTs(info.getCreate_time()))
                )
                .keyBy(PaymentInfo::getOrder_id);

        KeyedStream<OrderWideTable, Long> orderWideStream = streams
                .get(TOPIC_DWM_ORDER_WIDE)
                .map(ow -> JSON.parseObject(ow, OrderWideTable.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<OrderWideTable>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((info, ts) -> IncUtil.toTs(info.getCreate_time()))
                )
                .keyBy(OrderWideTable::getOrder_id);

        paymentInfoStream
                .intervalJoin(orderWideStream)
                .between(Time.minutes(-45), Time.seconds(10))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWideTable, PaymentWideTable>() {
                    @Override
                    public void processElement(PaymentInfo left,
                                               OrderWideTable right,
                                               Context ctx,
                                               Collector<PaymentWideTable> out) throws Exception {
                        out.collect(new PaymentWideTable(left, right));
                    }
                })
                .map(JSON::toJSONString)
                .addSink(FlinkSinkUtil.getKafkaSink(Constants.TOPIC_DWM_PAYMENT_WIDE));


    }
}
