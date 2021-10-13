package mall.streamer.app.dws;

import mall.streamer.bean.AppModelSQL;
import mall.streamer.bean.KeywordStats;
import mall.streamer.common.Constants;
import mall.streamer.function.IkAnalyzer;
import mall.streamer.function.KwProduct;
import mall.streamer.utils.FlinkSinkUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2021/10/13/16:08
 * @Description:
 */
public class DwsProductKeywordsApp extends AppModelSQL {
    public static void main(String[] args) {
        new DwsProductKeywordsApp().init(4005, 1, "DwsProductKeyWordStatsApp");
    }

    @Override
    public void run(StreamTableEnvironment tenv) {

        tenv.executeSql("create table product_stats(" +
                "   stt string, " +
                "   edt string, " +
                "   sku_name string, " +
                "   click_ct bigint, " +
                "   order_ct bigint, " +
                "   cart_ct bigint " +
                ")with(" +
                "   'connector' = 'kafka', " +
                "   'properties.bootstrap.servers' = 'hadoop162:9092,hadoop163:9092,hadoop164:9092', " +
                "   'properties.group.id' = 'DwsProductKeyWordStatsApp', " +
                "   'topic' = '" + Constants.TOPIC_DWS_PRODUCT_STATS + "', " +
                "   'scan.startup.mode' = 'latest-offset', " +
                "   'format' = 'json' " +
                ")");


        // 1. filtering
        Table t1 = tenv.sqlQuery("select" +
                " *" +
                "from product_stats " +
                "where click_ct > 0 " +
                "or order_ct > 0 " +
                "or cart_ct > 0 ");
        tenv.createTemporaryView("t1", t1);

        // 2. split the word
        tenv.createTemporaryFunction("ik_analyzer", IkAnalyzer.class);

        Table t2 = tenv.sqlQuery("select" +
                " stt, " +
                " edt, " +
                " word, " +
                " click_ct, " +
                " order_ct, " +
                " cart_ct " +
                "from t1 " +
                "join lateral table(ik_analyzer(sku_name)) on true");
        tenv.createTemporaryView("t2", t2);
        // 3. explode
        tenv.createTemporaryFunction("kw_product", KwProduct.class);

        Table t3 = tenv.sqlQuery("select" +
                " stt, " +
                " edt, " +
                " word, " +
                " source, " +
                " ct " +
                "from t2, " +
                " lateral table(kw_product(click_ct, order_ct, cart_ct))");
        tenv.createTemporaryView("t3", t3);
        // 4. aggregate
        Table table = tenv.sqlQuery("select" +
                " stt, " +
                " edt," +
                " word keyword, " +
                " source, " +
                " sum(ct) ct, " +
                " unix_timestamp() *1000 ts " +
                "from t3 " +
                "group by stt, edt, word, source");

        // 5. write to clickhouse
        tenv
                .toRetractStream(table, KeywordStats.class)
                .filter(t -> t.f0)
                .map(t -> t.f1)
                .addSink(FlinkSinkUtil.getClickHouseSink(
                        "gmall2021", "keyword_stats_2021", KeywordStats.class
                ));

    }
}
