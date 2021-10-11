package mall.streamer.bean;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2021/10/11/15:17
 * @Description:
 */
public abstract class AppModelSQL {
    public abstract void run(StreamTableEnvironment tenv);

    public void init(int port,
                     int parallelism,
                     String ck) {
        System.setProperty("HADOOP_USER_NAME", "root");

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(parallelism);

        // 1. exactly once
        env.enableCheckpointing(3000, CheckpointingMode.EXACTLY_ONCE);
        // 2. Checkpoint
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        env
                .getCheckpointConfig()
                .enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 3. state back end
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop162:8020/gmall2021/flink/ck/" + ck);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        run(tenv);

        try {
            env.execute(ck);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
