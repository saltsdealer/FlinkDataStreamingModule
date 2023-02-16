package mall.streamer.app.dwd;



import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import mall.streamer.bean.AppModelV1;
import mall.streamer.common.Constants;
import mall.streamer.utils.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2022/09/28/16:54
 * @Description:
 */
public class DwdLogApp extends AppModelV1 {

    public static final String PAGE = "page";
    public static final String START = "start";
    public static final String DISPLAY = "display";

    public static void main(String[] args) {
        // this is where the port, parallelism,ck,and groupid is implemented
        new DwdLogApp().init(2001,1,"dwdLogApp","dwdLogApp", Constants.TOPIC_ODS_LOG);
    }
    @Override
    public void run(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // process the data flow
        // 1. distinguish between old and new customers
        SingleOutputStreamOperator<JSONObject> validatedStream = distinguishNewOrOld(stream);

        // 2. to process the data into different stream
        HashMap<String, DataStream<JSONObject>> threeStreams = splitStream(validatedStream);
        // 2.1 and different topics
        writeToKafka(threeStreams);


    }

    private HashMap<String, DataStream<JSONObject>> splitStream(SingleOutputStreamOperator<JSONObject> validatedStream) {

        OutputTag<JSONObject> pageTag = new OutputTag<JSONObject>("page") {};
        OutputTag<JSONObject> displayTag = new OutputTag<JSONObject>("display") {};

        SingleOutputStreamOperator<JSONObject> pageStream = validatedStream.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject obj,
                                       Context ctx,
                                       Collector<JSONObject> out) throws Exception {

                JSONObject start = obj.getJSONObject("start");
                if (start != null) {
                    // start
                    out.collect(obj);
                } else {
                    JSONObject page = obj.getJSONObject("page");
                    if (page != null) {
                        ctx.output(pageTag, obj);
                    }

                    JSONArray displays = obj.getJSONArray("displays");
                    if (displays != null) {
                        // break the json cluster
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);

                            display.put("ts", obj.getLong("ts"));
                            display.putAll(obj.getJSONObject("common"));
                            display.putAll(obj.getJSONObject("page"));

                            ctx.output(displayTag, display);
                        }
                    }
                }
            }
        });

        HashMap<String, DataStream<JSONObject>> result = new HashMap<>();
        result.put(START, pageStream);
        result.put(PAGE, pageStream.getSideOutput(pageTag));
        result.put(DISPLAY, pageStream.getSideOutput(displayTag));
        return result;


    }

    private SingleOutputStreamOperator<JSONObject> distinguishNewOrOld(DataStreamSource<String> stream) {
        /* if found the first time visit records,
            then use state to show if it's the first time
            state + window put is_new 1
         */

        return stream
                .map(JSON::parseObject)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((obj, ts) -> obj.getLong("ts"))
                )
                .keyBy(obj -> obj.getJSONObject("common").getString("mid"))
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>() {

                    private ValueState<Boolean> isFirstWindowState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        isFirstWindowState = getRuntimeContext()
                                .getState(new ValueStateDescriptor<Boolean>(
                                        "isFirstWindowState",
                                        Boolean.class
                                ));
                    }

                    @Override
                    public void process(String key,
                                        Context ctx,
                                        Iterable<JSONObject> elements,
                                        Collector<JSONObject> out) throws Exception {
                        // sort by timestamp, the smallest to 1,
                        // others to 0
                        if (isFirstWindowState.value() == null) {
                            //update state
                            isFirstWindowState.update(true);
                            List<JSONObject> list = IncUtil.toList(elements);

                            list.sort(Comparator.comparing(o -> o.getLong("ts")));

                            for (int i = 0; i < list.size(); i++) {
                                JSONObject obj = list.get(i);
                                if (i == 0) {
                                    obj.getJSONObject("common").put("is_new", "1");
                                }else{
                                    obj.getJSONObject("common").put("is_new", "0");
                                }
                                out.collect(obj);
                            }

                        } else {
                            //not the first window
                            for (JSONObject obj : elements) {
                                obj.getJSONObject("common").put("is_new", "0");
                                out.collect(obj);

                            }

                        }

                    }
                });
    }

    private void writeToKafka(HashMap<String, DataStream<JSONObject>> threeStreams) {

        threeStreams
                .get(PAGE)
                .map(JSONAware::toJSONString)
                .addSink(FlinkSinkUtil.getKafkaSink(Constants.TOPIC_DWD_PAGE));
        threeStreams
                .get(START)
                .map(JSONAware::toJSONString)
                .addSink(FlinkSinkUtil.getKafkaSink(Constants.TOPIC_DWD_START));
        threeStreams
                .get(DISPLAY)
                .map(JSONAware::toJSONString)
                .addSink(FlinkSinkUtil.getKafkaSink(Constants.TOPIC_DWD_DISPLAY));

    }
}
