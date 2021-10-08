package mall.streamer.sink;

import com.alibaba.fastjson.JSONObject;

import mall.streamer.bean.TableProcess;
import mall.streamer.common.Constants;
import mall.streamer.utils.JdbcUtil;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2021/09/29/15:27
 * @Description:
 */
public class PhoenixSink extends RichSinkFunction<Tuple2<JSONObject, TableProcess>>{
    private Connection conn;
    private ValueState<Boolean> isFirstState;

    @Override
    public void open(Configuration parameters) throws Exception {

        /*
        Create Phoenix connections

        */
        String url = Constants.PHOENIX_URL;
        conn = JdbcUtil.getPhoenixConnection(url);

        isFirstState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("isFirstState", Boolean.class));

    }

    // process the data
    @Override
    public void invoke(Tuple2<JSONObject, TableProcess> value,
                       Context ctx) throws Exception {
        // 1. check if table already exists
        checkTable(value);
        // 2. write data to hbase
        writeToPhoenix(value);

    }

    private void writeToPhoenix(Tuple2<JSONObject, TableProcess> value) throws SQLException {
        JSONObject data = value.f0;
        TableProcess tp = value.f1;

        // 1. the sql should be like "id,name"
        // upsert into t(id, name) values(?,?)
        StringBuilder sql = new StringBuilder();
        sql
                .append("upsert into ")
                .append(tp.getSink_table())
                .append("(")

                .append(tp.getSink_columns())
                .append(")values(")
                .append(tp.getSink_columns().replaceAll("[^,]+", "?"))
                .append(")");

        // 2. PrepareStatement
        PreparedStatement ps = conn.prepareStatement(sql.toString());
        // 3. appoint value to placeholder
        String[] columnNames = tp.getSink_columns().split(",");
        for (int i = 0; i < columnNames.length; i++) {
            Object v = data.get(columnNames[i]);
            // to avoid null -> "null"
            ps.setString(i + 1, v == null ? null : v.toString());
        }
        System.out.println("insert: " + sql.toString());
        // execute
        ps.execute();
        conn.commit();
        ps.close();

    }

    private void checkTable(Tuple2<JSONObject, TableProcess> value) throws SQLException, IOException {

        if (isFirstState.value() == null) {
            // for test : System.out.println("testing");
            // only execute once
            TableProcess tp = value.f1;

            // create table if not exists t(id varchar, name varchar, constraint pk primary key(id, name)) SALT_BUCKETS = 4;
            StringBuilder sql = new StringBuilder();
            sql
                    .append("create table if not exists ")
                    .append(tp.getSink_table())
                    .append("(")
                    .append(tp.getSink_columns().replaceAll(",", " varchar,"))
                    .append(" varchar, constraint pk primary key(")
                    .append(tp.getSink_pk() == null ? "id" : tp.getSink_pk())
                    .append("))")
                    .append(tp.getSink_extend() == null ? "" : tp.getSink_extend());

            PreparedStatement ps = conn.prepareStatement(sql.toString());
            ps.execute();
            conn.commit();
            ps.close();

            isFirstState.update(true);
        }

    }

    @Override
    public void close() throws Exception {
        if (conn != null) {
            conn.close();
        }
    }

}
