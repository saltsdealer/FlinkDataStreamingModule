package mall.streamer.app.dws;

import mall.streamer.bean.AppModelSQL;
import mall.streamer.bean.ProvinceStats;
import mall.streamer.common.Constants;
import mall.streamer.utils.FlinkSinkUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2021/10/11/15:38
 * @Description:
 */
public class DwsProvinceStatsApp extends AppModelSQL {
    public static void main(String[] args) {
        new DwsProvinceStatsApp().init(4003, 1, "DwsProvinceStatsApp");

    }

    @Override
    public void run(StreamTableEnvironment tenv) {

        tenv.executeSql("create table order_wide(" +
                "   province_id bigint, " +
                "   province_name string, " +
                "   province_area_code string, " +
                "   province_iso_code string, " +
                "   province_3166_2_code string, " +
                "   order_id bigint, " +
                "   split_total_amount decimal(20, 2), " +
                "   create_time string, " +
                // need the method implemented
                "   et as to_timestamp(create_time), " +
                "   watermark for et as et - interval '5' second " +
                ")with(" +
                "   'connector' = 'kafka', " +
                "   'properties.bootstrap.servers' = 'hadoop162:9092,hadoop163:9092,hadoop164:9092', " +
                "   'properties.group.id' = 'DwsProvinceStatsApp', " +
                "   'topic' = '" + Constants.TOPIC_DWM_ORDER_WIDE + "', " +
                "   'scan.startup.mode' = 'latest-offset', " +
                "   'format' = 'json' " +
                ")");

        // 2. aggregation
        Table resultTable = tenv.sqlQuery("select" +
                " date_format(tumble_start(et, interval '5' second), 'yyyy-MM-dd HH:mm:ss') stt," +
                " date_format(tumble_end(et, interval '5' second), 'yyyy-MM-dd HH:mm:ss') edt," +
                " province_id, " +
                " province_name, " +
                " province_area_code area_code," +
                " province_iso_code iso_code, " +
                " province_3166_2_code iso_3166_2, " +
                " sum(split_total_amount) order_amount, " +
                " count(distinct(order_id)) order_count, " +
                " unix_timestamp() *1000 ts " +
                " from order_wide " +
                " group by " +
                " province_id, " +
                " province_name, " +
                " province_area_code," +
                " province_iso_code, " +
                " province_3166_2_code, " +
                " tumble(et, interval '5' second)");

        //3. write to click house
        tenv
                .toRetractStream(resultTable, ProvinceStats.class)
                .filter(t -> t.f0)
                .map(t -> t.f1)
                .addSink(FlinkSinkUtil.getClickHouseSink(
                        "gmall2021", "province_stats_2021", ProvinceStats.class
                ));
    }
}
