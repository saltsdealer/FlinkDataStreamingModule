package mall.streamer.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import mall.streamer.bean.TableProcess;
import mall.streamer.bean.AppModelV1;
import mall.streamer.common.Constants;
import mall.streamer.utils.FlinkSinkUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2021/09/28/16:54
 * @Description:
 */
public class DwdDbApp extends AppModelV1 {

    public static void main(String[] args) {
        // this is where the port, parallelism,ck,and groupid is implemented
        new DwdDbApp().init(2002, 1, "DwdDbApp", "DwdDbApp", Constants.TOPIC_ODS_DB);
    }


    @Override
    public void run(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        /*
        1. etl
        2. read config
        3. connect
        4. filter of no sink needs
        5. to different stream
        6. to different sink
         */

        SingleOutputStreamOperator<JSONObject> etledStream = etl(stream);
        SingleOutputStreamOperator<TableProcess> tpStream = readProcessTable(env);
        SingleOutputStreamOperator<Tuple2<JSONObject,TableProcess>> connectedStream = connectStream(etledStream,tpStream);
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> filteredStream = filterColumns(connectedStream);
        Tuple2<SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>>, DataStream<Tuple2<JSONObject, TableProcess>>> kafkaHbaseStreams = dynamicSplit(filteredStream);
        writeToKafka(kafkaHbaseStreams.f0);
        writeToHbase(kafkaHbaseStreams.f1);
    }

    private void writeToHbase(DataStream<Tuple2<JSONObject, TableProcess>> stream) {
        /*
        1. create the table when it first came
        2. write the data to hbase via jdbc
         */
        stream
                .keyBy(t -> t.f1.getSink_table())
                .addSink(FlinkSinkUtil.getPhoenixSink());
    }

    private void writeToKafka(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> stream) {
        stream.addSink(FlinkSinkUtil.getKafkaSink());
    }

    private Tuple2<SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>>,
                                                DataStream<Tuple2<JSONObject, TableProcess>>>
            dynamicSplit(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> filteredStream) {

        OutputTag<Tuple2<JSONObject, TableProcess>> hbaseTag = new OutputTag<Tuple2<JSONObject, TableProcess>>("hbaseTag") {};
        // two streams, one to kafka, one to hbase
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> kafkaStream = filteredStream
                .keyBy(t -> t.f1.getSource_table())
                .process(new KeyedProcessFunction<String, Tuple2<JSONObject, TableProcess>, Tuple2<JSONObject, TableProcess>>() {
                    @Override
                    public void processElement(Tuple2<JSONObject, TableProcess> value,
                                               Context ctx, Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                        String sinkType = value.f1.getSink_type();

                        if (Constants.DWD_SINK_KAFKA.equals(sinkType)) {
                            out.collect(value);
                        } else if (Constants.DWD_SINK_HBASE.equals(sinkType)) {
                            ctx.output(hbaseTag, value);
                        }
                    }
                });

        return Tuple2.of(kafkaStream, kafkaStream.getSideOutput(hbaseTag));

    }


    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> filterColumns(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectedStream) {
        return connectedStream
                .map(new MapFunction<Tuple2<JSONObject, TableProcess>, Tuple2<JSONObject, TableProcess>>() {
                    @Override
                    public Tuple2<JSONObject, TableProcess> map(Tuple2<JSONObject, TableProcess> t) throws Exception {
                        JSONObject data = t.f0;
                        TableProcess tp = t.f1;
                        // id,name,...
                        String columns = tp.getSink_columns();

                        // id
                        data.keySet().removeIf(column -> !columns.contains(column));
                        return t;
                    }
                });
    }


    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream
                .map(data -> JSON.parseObject(data.replaceAll("bootstrap-", "")))
                .filter(obj ->
                        obj.getString("database") != null
                                && obj.getString("table") != null
                                && obj.getString("data") != null
                                && obj.getString("data").length() > 10
                                && (obj.getString("type").equals("update") || obj.getString("type").equals("insert"))
                );
    }


    private SingleOutputStreamOperator<TableProcess> readProcessTable(StreamExecutionEnvironment env) {
        StreamTableEnvironment env1 = StreamTableEnvironment.create(env);
        env1.executeSql("create table table_process(" +
                "   source_table string, " +
                "   operate_type string, " +
                "   sink_type string, " +
                "   sink_table string, " +
                "   sink_columns string, " +
                "   sink_pk string, " +
                "   sink_extend string," +
                "   PRIMARY KEY (`source_table`,`operate_type`)NOT ENFORCED" +
                ")with(" +
                "   'connector' = 'mysql-cdc',\n" +
                "   'hostname' = 'hadoop102',\n" +
                "   'port' = '3306',\n" +
                "   'username' = 'root',\n" +
                "   'password' = 'aaaaaa',\n" +
                "   'database-name' = 'mall2021',\n" +
                "   'table-name' = 'table_process', " +
                "   'debezium.snapshot.mode' = 'initial' " +
                ")");

        Table table = env1.sqlQuery("select * from table_process");

        return env1
                .toRetractStream(table, TableProcess.class)
                .filter(t -> t.f0)
                .map(t -> t.f1);

    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectStream(SingleOutputStreamOperator<JSONObject> etledStream, SingleOutputStreamOperator<TableProcess> tpStream) {
        MapStateDescriptor<String, TableProcess> tpDesc =
                new MapStateDescriptor<>("tpState", String.class, TableProcess.class);
        // broadcast the stream
        // user_info:insert   ->  TableProcess
        BroadcastStream<TableProcess> tpBCStream = tpStream
                .broadcast(tpDesc);
        // connect the tpStream
        return etledStream
                .keyBy(obj -> obj.getString("table"))
                .connect(tpBCStream)
                .process(new KeyedBroadcastProcessFunction<String, JSONObject, TableProcess, Tuple2<JSONObject, TableProcess>>() {
                    @Override
                    public void processElement(JSONObject obj,
                                               ReadOnlyContext ctx,
                                               Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {

                        ReadOnlyBroadcastState<String, TableProcess> tpState = ctx.getBroadcastState(tpDesc);


                        String key = obj.getString("table") + ":" + obj.getString("type");

                        TableProcess tp = tpState.get(key);

                        if (tp != null) {

                            out.collect(Tuple2.of(obj.getJSONObject("data"), tp));
                        }

                    }

                    @Override
                    public void processBroadcastElement(TableProcess tp,
                                                        Context ctx,
                                                        Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {

                        BroadcastState<String, TableProcess> tpState = ctx.getBroadcastState(tpDesc);
                        tpState.put(tp.getSource_table() + ":" + tp.getOperate_type(), tp);

                    }
                });
    }
}
