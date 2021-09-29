package mall.streamer.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;
/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2021/09/28/16:36
 * @Description:
 */
public class flinkSourceUtil {
    public static FlinkKafkaConsumer<String> getKafkaSource(String groupId, String topic){
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        props.setProperty("group.id", groupId);
        props.setProperty("auto.offset.reset", "latest");
        props.setProperty("isolation.level", "read_committed");

        return new FlinkKafkaConsumer<String>(
                topic,
                new SimpleStringSchema(),
                props
        );
    }
}
