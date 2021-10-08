package mall.streamer.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import mall.streamer.bean.AppModelV1;
import mall.streamer.bean.AppModelV2;
import mall.streamer.common.Constants;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2021/09/28/16:39
 * @Description:
 */

public class DwmUserJumpDetailApp extends AppModelV1 {
    public static void main(String[] args) {
        new DwmUserJumpDetailApp().init(3002,
                                        1,
                                        "DwmUserJumpDetailApp",
                                        "DwmUserJumpDetailApp",
                                        Constants.TOPIC_DWD_PAGE);
        
    }

    @Override
    public void run(StreamExecutionEnvironment env,
                    DataStreamSource<String> stream) {

        stream =
                env.fromElements(
                        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
                        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":14000} ",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000}",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                                "\"home\"},\"ts\":14999} ",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                                "\"detail\"},\"ts\":50000} "
                );

        KeyedStream<JSONObject, String> keyedStream = stream
                .map(JSON::parseObject)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((obj, ts) -> obj.getLong("ts"))
                )
                .keyBy(obj -> obj.getJSONObject("common").getString("mid"));

        //1. 定义模式
        Pattern<JSONObject, JSONObject> pattern = Pattern
                .<JSONObject>begin("entry")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {

                        JSONObject page = value.getJSONObject("page");
                        return page.getString("last_page_id") == null || page.getString("last_page_id").length() == 0;
                    }
                })
                .next("secondPage")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        JSONObject page = value.getJSONObject("page");
                        String lastPageId = page.getString("last_page_id");
                        return lastPageId != null && lastPageId.length() > 0;
                    }
                })
                .within(Time.seconds(5));

        //2. 把模式作用到流上
        PatternStream<JSONObject> ps = CEP.pattern(keyedStream, pattern);
        // 3. 从模式流中获取需要的数据
        SingleOutputStreamOperator<JSONObject> normal = ps
                .select(
                        new OutputTag<JSONObject>("timeout") {},
                        new PatternTimeoutFunction<JSONObject, JSONObject>() {
                            @Override
                            public JSONObject timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp) throws Exception {
                                return pattern.get("entry").get(0);
                            }

                        },
                        new PatternSelectFunction<JSONObject, JSONObject>() {
                            @Override
                            public JSONObject select(Map<String, List<JSONObject>> pattern) throws Exception {
                                return null;
                            }
                        }
                );

        normal.getSideOutput(new OutputTag<JSONObject>("timeout") {}).print();

    }
}
