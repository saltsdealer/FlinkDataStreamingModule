package mall.streamer.app.dws;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import mall.streamer.bean.AppModelV2;
import mall.streamer.bean.VisitorStats;
import mall.streamer.utils.FlinkSinkUtil;
import mall.streamer.utils.IncUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.HashMap;

import static mall.streamer.common.Constants.*;


/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2021/10/10/19:28
 * @Description:
 */
public class DwsVisitorStatsApp extends AppModelV2 {
    public static void main(String[] args) {
        new DwsVisitorStatsApp().init(
                4001,
                1,
                "DwsVisitorStatsApp", "DwsVisitorStatsApp",
                TOPIC_DWD_PAGE, TOPIC_DWM_UV, TOPIC_DWM_USER_JUMP_DETAIL);
    }

    @Override
    public void run(StreamExecutionEnvironment env,
                    HashMap<String, DataStreamSource<String>> streams) {
        /*streams.get(TOPIC_DWD_PAGE).print(TOPIC_DWD_PAGE);
        streams.get(TOPIC_DWM_UV).print(TOPIC_DWM_UV);
        streams.get(TOPIC_DWM_USER_JUMP_DETAIL).print(TOPIC_DWM_USER_JUMP_DETAIL);*/

        // 1. stream joins
        DataStream<VisitorStats> vsStream = parseAndUnionOne(streams);
        SingleOutputStreamOperator<VisitorStats> vsAggregatedStream = aggregate(vsStream);
        // 3. write to ck
        writeToClickHouse(vsAggregatedStream);

    }

    private void writeToClickHouse(SingleOutputStreamOperator<VisitorStats> stream) {
        stream.addSink(FlinkSinkUtil.getClickHouseSink("gmall2021", "visitor_stats_2021", VisitorStats.class));
    }

    private SingleOutputStreamOperator<VisitorStats> aggregate(DataStream<VisitorStats> vsStream) {
        return vsStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                                .withTimestampAssigner((vs, ts) -> vs.getTs())
                )
                .keyBy(vs -> vs.getVc() + "_" + vs.getCh() + "_" + vs.getAr() + "_" + vs.getIs_new())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sideOutputLateData(new OutputTag<VisitorStats>("late"){})
                .reduce(
                        new ReduceFunction<VisitorStats>() {
                            @Override
                            public VisitorStats reduce(VisitorStats vs1,
                                                       VisitorStats vs2) throws Exception {
                                vs1.setPv_ct(vs1.getPv_ct() + vs2.getPv_ct());
                                vs1.setUv_ct(vs1.getUv_ct() + vs2.getUv_ct());
                                vs1.setSv_ct(vs1.getSv_ct() + vs2.getSv_ct());
                                vs1.setUj_ct(vs1.getUj_ct() + vs2.getUj_ct());
                                vs1.setDur_sum(vs1.getDur_sum() + vs2.getDur_sum());

                                return vs1;
                            }
                        },
                        new ProcessWindowFunction<VisitorStats, VisitorStats, String, TimeWindow>() {

                            @Override
                            public void process(String key,
                                                Context ctx,
                                                Iterable<VisitorStats> elements,
                                                Collector<VisitorStats> out) throws Exception {
                                VisitorStats vs = elements.iterator().next();

                                TimeWindow w = ctx.window();
                                String stt= IncUtil.toDataTimeString(w.getStart());
                                String edt= IncUtil .toDataTimeString(w.getEnd());

                                vs.setStt(stt);
                                vs.setEdt(edt);
                                vs.setTs(System.currentTimeMillis()); // 更新统计时间
                                out.collect(vs);
                            }
                        }
                );
    }

    private DataStream<VisitorStats> parseAndUnionOne(HashMap<String, DataStreamSource<String>> streams) {
        // 1.page visits and duration
        SingleOutputStreamOperator<VisitorStats> pvAndDurSumStream = streams
                .get(TOPIC_DWD_PAGE)
                .map(json -> {
                    JSONObject obj = JSON.parseObject(json);

                    JSONObject common = obj.getJSONObject("common");
                    String vc = common.getString("vc");
                    String ch = common.getString("ch");
                    String ar = common.getString("ar");
                    String is_new = common.getString("is_new");

                    Long ts = obj.getLong("ts");

                    Long dur_sum = obj.getJSONObject("page").getLong("during_time");

                    return new VisitorStats(
                            "", "",
                            vc, ch, ar, is_new,
                            0L, 1L, 0L, 0L, dur_sum,
                            ts);

                });

        // 2. uv
        SingleOutputStreamOperator<VisitorStats> uvStream = streams
                .get(TOPIC_DWM_UV)
                .map(json -> {
                    JSONObject obj = JSON.parseObject(json);

                    JSONObject common = obj.getJSONObject("common");
                    String vc = common.getString("vc");
                    String ch = common.getString("ch");
                    String ar = common.getString("ar");
                    String is_new = common.getString("is_new");

                    Long ts = obj.getLong("ts");

                    return new VisitorStats(
                            "", "",
                            vc, ch, ar, is_new,
                            1L, 0L, 0L, 0L, 0L,
                            ts);

                });
        // 3. uj count
        SingleOutputStreamOperator<VisitorStats> ujStream = streams
                .get(TOPIC_DWM_USER_JUMP_DETAIL)
                .map(json -> {
                    JSONObject obj = JSON.parseObject(json);

                    JSONObject common = obj.getJSONObject("common");
                    String vc = common.getString("vc");
                    String ch = common.getString("ch");
                    String ar = common.getString("ar");
                    String is_new = common.getString("is_new");

                    Long ts = obj.getLong("ts");

                    return new VisitorStats(
                            "", "",
                            vc, ch, ar, is_new,
                            0L, 0L, 0L, 1L, 0L,
                            ts);

                });

        // 4. sv
        SingleOutputStreamOperator<VisitorStats> svStream = streams
                .get(TOPIC_DWD_PAGE)
                .flatMap(new FlatMapFunction<String, VisitorStats>() {
                    @Override
                    public void flatMap(String json, Collector<VisitorStats> out) throws Exception {
                        JSONObject obj = JSON.parseObject(json);

                        String lastPageId = obj.getJSONObject("page").getString("last_page_id");
                        if (lastPageId == null || lastPageId.length() == 0) {
                            JSONObject common = obj.getJSONObject("common");
                            String vc = common.getString("vc");
                            String ch = common.getString("ch");
                            String ar = common.getString("ar");
                            String is_new = common.getString("is_new");

                            Long ts = obj.getLong("ts");

                            VisitorStats vs = new VisitorStats(
                                    "", "",
                                    vc, ch, ar, is_new,
                                    0L, 0L, 1L, 0L, 0L,
                                    ts);
                            out.collect(vs);

                        }

                    }
                });

        return pvAndDurSumStream.union(
                uvStream,
                ujStream,
                svStream
        );

    }
}
