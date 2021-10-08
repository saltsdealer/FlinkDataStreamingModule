package mall.streamer.bean;

import mall.streamer.utils.FlinkSourceUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2021/10/08/11:20
 * @Description: for the base app model of DWM
 */
public abstract class AppModelV2 {
    public abstract void run(StreamExecutionEnvironment env,
                             HashMap<String, DataStreamSource<String>> streams);

    public void init(int port, int parallelism, String ck, String groupId, String topic, String ... otherTopics) {
        System.setProperty("HADOOP_USER_NAME", "root");

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(parallelism);

        // 1. Checkpoint settings
        env.enableCheckpointing(3000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 2. Maintain the checkpoint
        env
                .getCheckpointConfig()
                .enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new HashMapStateBackend());
        // 3. set the storage path
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/mall/flink/ck/" + ck);

        ArrayList<String> topics = new ArrayList<>();
        topics.add(topic);
        topics.addAll(Arrays.asList(otherTopics));

        HashMap<String, DataStreamSource<String>> streams = new HashMap<>();
        for (String t : topics) {
            DataStreamSource<String> stream = env.addSource(FlinkSourceUtil.getKafkaSource(groupId, t));
            streams.put(t, stream);
        }

        run(env, streams);
        try {
            env.execute(ck);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
