package mall.streamer.bean;

import mall.streamer.utils.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2021/09/28/16:39
 * @Description: a model for apps to use
 */
public abstract class AppModelV1 {
    public abstract void run(StreamExecutionEnvironment env,
                             DataStreamSource<String> stream);

    public void init(int port, int parallelism, String ck, String groupId, String topic) {
        System.setProperty("HADOOP_USER_NAME", "user");

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(parallelism);

        // 1. exactly once settings(default)   checkpoint per 5000ms
        env.enableCheckpointing(3000, CheckpointingMode.EXACTLY_ONCE);
        // 2. Checkpoint preserving time
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 3. externalized checkpoints for recover
        env
                .getCheckpointConfig()
                .enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 4. state backend
        env.setStateBackend(new HashMapStateBackend());
        // the hdfs location to store the checkpoint
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall2021/flink/ck/" + ck);

        DataStreamSource<String> stream = env.addSource(FlinkSourceUtil.getKafkaSource(groupId, topic));

        run(env, stream);
        try {
            env.execute(ck);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
