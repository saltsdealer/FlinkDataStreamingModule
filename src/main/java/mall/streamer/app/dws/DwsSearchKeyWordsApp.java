package mall.streamer.app.dws;

import mall.streamer.bean.AppModelSQL;
import mall.streamer.bean.KeywordStats;
import mall.streamer.common.Constants;
import mall.streamer.function.IkAnalyzer;
import mall.streamer.utils.FlinkSinkUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2021/10/13/11:45
 * @Description:
 */
public class DwsSearchKeyWordsApp extends AppModelSQL {
    public static void main(String[] args) {
        new DwsSearchKeyWordsApp().init(4004, 1, "DwsSearchKeyWordStatsApp");
    }

    @Override
    public void run(StreamTableEnvironment tenv) {

        tenv.executeSql("create table page_log(" +
                "   common map<string, string>, " +
                "   page map<string, string>," +
                "   ts bigint, " +
                "   et as to_timestamp(from_unixtime(ts/1000))," +
                "   watermark for et as et - interval '3' second " +
                ")with(" +
                "   'connector' = 'kafka', " +
                "   'properties.bootstrap.servers' = 'hadoop162:9092,hadoop163:9092,hadoop164:9092', " +
                "   'properties.group.id' = 'DwsSearchKeyWordStatsApp', " +
                "   'topic' = '" + Constants.TOPIC_DWD_PAGE + "', " +
                "   'scan.startup.mode' = 'latest-offset', " +
                "   'format' = 'json' " +
                ")");

        // 1. data filtering
        Table t1 = tenv.sqlQuery("select" +
                " page['item'] keyword, " +
                " et " +
                "from page_log " +
                "where page['page_id']='good_list' " +
                " and page['item'] is not null " +
                " and page['item_type']='keyword'");
        tenv.createTemporaryView("t1", t1);

        // 2. split the words using IkAnalyzer
        tenv.createTemporaryFunction("ik_analyzer", IkAnalyzer.class);
        Table t2 = tenv.sqlQuery("select " +
                " keyword, " +
                " word, " +
                " et " +
                "from t1 " +
                "join lateral table(ik_analyzer(keyword)) on true");

        tenv.createTemporaryView("t2", t2);
        // 3. window
        Table t3 = tenv.sqlQuery("select " +
                " date_format(tumble_start(et, interval '10' second), 'yyyy-MM-dd HH:mm:ss') stt, " +
                " date_format(tumble_end(et, interval '10' second), 'yyyy-MM-dd HH:mm:ss') edt, " +
                " word keyword, " +
                " 'search' source, " +
                " count(word) ct, " +
                " unix_timestamp() *1000 ts " +  // count(*) count(word)  sum(1)
                "from t2 " +
                "group by " +
                " word, tumble(et, interval '10' second)");

        // 4. write to clickhouse
        tenv
                .toRetractStream(t3, KeywordStats.class)
                .filter(t -> t.f0)
                .map(t -> t.f1)
                .addSink(FlinkSinkUtil.getClickHouseSink(
                        "database", "keyword_stats_2021", KeywordStats.class
                ));

    }
}
