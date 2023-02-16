package mall.streamer.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import mall.streamer.bean.AppModelV1;
import mall.streamer.common.Constants;
import mall.streamer.utils.FlinkSinkUtil;
import mall.streamer.utils.IncUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2022/09/28/16:39
 * @Description:
 */

public class DwmUvApp extends AppModelV1 {
    public static void main(String[] args) {
        new DwmUvApp().init(3001, 1, "DwmUvApp", "DwmUvApp", Constants.TOPIC_DWD_PAGE);
    }
    
    @Override
    public void run(StreamExecutionEnvironment env,
                    DataStreamSource<String> stream) {
        stream
            .map(JSON::parseObject)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((obj, ts) -> obj.getLong("ts"))
            )
            .keyBy(obj -> obj.getJSONObject("common").getString("mid"))
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .process(new ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>() {
    
                private SimpleDateFormat sdf;
                private ValueState<Long> firstVisitState;
    
                @Override
                public void open(Configuration parameters) throws Exception {
                    firstVisitState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("firstVisitState", Long.class));
                    sdf = new SimpleDateFormat("yyyy-MM-dd");
                }
    
                @Override
                public void process(String key,
                                    Context ctx,
                                    Iterable<JSONObject> elements,
                                    Collector<JSONObject> out) throws Exception {
                    // see if it is a two day event
                    String today = sdf.format(ctx.window().getStart());

                    long yesterdayMs = firstVisitState.value() == null ? 0L : firstVisitState.value();
                    String yesterday = sdf.format(yesterdayMs);
                    if(!today.equals(yesterday)){
                        firstVisitState.clear();
                    }
    
                    if (firstVisitState.value() == null) {
                        List<JSONObject> list = IncUtil.toList(elements);
//                        Collections.min(list, (o1, o2) -> o1.getLong("ts").compareTo(o2.getLong("ts")))
                        JSONObject min = Collections.min(list, Comparator.comparing(o -> o.getLong("ts")));
                        out.collect(min);
                        firstVisitState.update(min.getLong("ts"));
                    }
                }
            })
            .map(JSONAware::toJSONString)
            .addSink(FlinkSinkUtil.getKafkaSink(Constants.TOPIC_DWM_UV));
    }
}
