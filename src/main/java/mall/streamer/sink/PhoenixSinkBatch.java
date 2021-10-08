package mall.streamer.sink;

import com.alibaba.fastjson.JSONObject;
import mall.streamer.bean.TableProcess;
import mall.streamer.common.Constants;
import mall.streamer.utils.JdbcUtil;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.api.java.tuple.Tuple2;

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
public class PhoenixSinkBatch extends RichSinkFunction<Tuple2<JSONObject, TableProcess>> {
    private Connection conn;
    private ValueState<Boolean> isFirstState;
    private int count;
    private ValueState<PreparedStatement> psState;

    //
    @Override
    public void open(Configuration parameters) throws Exception {

        String url = Constants.PHOENIX_URL;
        conn = JdbcUtil.getPhoenixConnection(url);

        isFirstState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("isFirstState", Boolean.class));

        psState = getRuntimeContext().getState(new ValueStateDescriptor<PreparedStatement>("psState", PreparedStatement.class));

        count = 0;

    }

    // process the data
    @Override
    public void invoke(Tuple2<JSONObject, TableProcess> value,
                       Context ctx) throws Exception {
        checkTable(value);
        writeToPhoenix(value);

    }

    private void writeToPhoenix(org.apache.flink.api.java.tuple.Tuple2<JSONObject, TableProcess> value) throws SQLException, IOException {
        JSONObject data = value.f0;
        TableProcess tp = value.f1;

        if (psState.value() == null) {
            // 1. "id,name"
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

            PreparedStatement ps = conn.prepareStatement(sql.toString());

            psState.update(ps);
        }

        PreparedStatement ps = psState.value();

        String[] columnNames = tp.getSink_columns().split(",");
        for (int i = 0; i < columnNames.length; i++) {
            Object v = data.get(columnNames[i]);

            ps.setString(i + 1, v == null ? null : v.toString());
        }

        ps.addBatch();
        if(++count % 10 == 0){
            ps.executeBatch();
            conn.commit();
        }
        ps.close();

    }

    private void checkTable(org.apache.flink.api.java.tuple.Tuple2<JSONObject, TableProcess> value) throws SQLException, IOException {

        if (isFirstState.value() == null) {
            System.out.println("xxxx");

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
