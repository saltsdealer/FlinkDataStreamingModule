package mall.streamer.utils;

import mall.streamer.bean.TableProcess;
import mall.streamer.sink.PhoenixSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.alibaba.fastjson.JSONObject;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
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
}
