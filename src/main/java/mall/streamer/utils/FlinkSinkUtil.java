package mall.streamer.utils;

import com.alibaba.fastjson.JSONObject;
import lombok.SneakyThrows;
import mall.streamer.bean.TableProcess;
import mall.streamer.bean.VisitorStats;
import mall.streamer.common.Constants;
import mall.streamer.sink.PhoenixSink;
import lombok.SneakyThrows;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2021/09/28/16:33
 * @Description:
 */
public class FlinkSinkUtil {
    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop162:9092,hadoop163:9092,hadoop164:9092");
        props.put("transaction.timeout.ms", 15 * 60 * 1000 + "");

        return new FlinkKafkaProducer<String>(
                "default",
                new KafkaSerializationSchema<String>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(String element,
                                                                    @Nullable Long timestamp) {
                        return new ProducerRecord<>(topic, element.getBytes(StandardCharsets.UTF_8));
                    }
                },
                props,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }
    public static SinkFunction<Tuple2<JSONObject, TableProcess>> getPhoenixSink() {

        return new PhoenixSink();
    }

    public static SinkFunction<Tuple2<JSONObject, TableProcess>> getKafkaSink() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        props.put("transaction.timeout.ms", 15 * 60 * 1000 + "");



        return new FlinkKafkaProducer<Tuple2<JSONObject, TableProcess>>(
                "default",
                new KafkaSerializationSchema<Tuple2<JSONObject, TableProcess>>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(Tuple2<JSONObject, TableProcess> element,
                                                                    @Nullable Long timestamp) {
                        JSONObject data = element.f0;
                        TableProcess tp = element.f1;
                        return new ProducerRecord<>(tp.getSink_table(), data.toJSONString().getBytes(StandardCharsets.UTF_8));
                    }
                },
                props,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }


    public static <T> SinkFunction<T> getClickHouseSink(String db,
                                                        String tableName,
                                                        Class<T> tClass) {
        String driver = Constants.CLICKHOUSE_DRIVER;
        String url = Constants.CLICKHOUSE_URL_PRE + db;
        // TODO insert into t(a, b,c) values(?,?,?)
        StringBuilder sql = new StringBuilder();

        sql
                .append("insert into ")
                .append(tableName)
                .append("(");
        // 拼接属性名
        Field[] fields = tClass.getDeclaredFields();
        for (Field field : fields) {
            sql
                    .append(field.getName())
                    .append(",");
        }
        sql.deleteCharAt(sql.length() - 1);
        sql.append(") values(");
        // 拼接 ?
        for (Field field : fields) {
            sql.append("?,");
        }
        sql.deleteCharAt(sql.length() - 1);
        sql.append(")");

        System.out.println(sql.toString());

        return getJDBCSink(driver, url, sql.toString());

    }

    public static void main(String[] args) {
        getClickHouseSink("", "a", VisitorStats.class);
    }

    public static <T> SinkFunction<T> getJDBCSink(String driver,
                                                  String url,
                                                  String sql) {
        return JdbcSink.sink(
                sql,
                new JdbcStatementBuilder<T>() {
                    @SneakyThrows
                    @Override
                    public void accept(PreparedStatement ps,
                                       T t) throws SQLException {
                        // 3钟办法获取字节码:  Class.forName("....")  User.class  对象.getClass
                        Class<?> tClass = t.getClass();
                        Field[] fields = tClass.getDeclaredFields(); // 获取所有属性, 包含私有属性
                        for (int i = 1; i <= fields.length; i++) {
                            Field field = fields[i - 1];
                            field.setAccessible(true);  // 允许访问私有属性
                            ps.setObject(i, field.get(t)); // 给站位符赋值  field.get(t): 获取这个属性的值  t.getField()
                        }
                    }
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchIntervalMs(1000) // 默认是0 . 表示不会按照时间写入到数据库
                        .withBatchSize(1024) // 5000
                        .withMaxRetries(3) // 默认是3
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(url)
                        .withDriverName(driver)
                        .build()
        );
    }

}
