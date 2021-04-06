package me.arnu.flink.demo;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class Main {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "vm1:9091,vm2:9092,vm3:9092");
        // props.put("bootstrap.servers", "hadoop-01:9092");
        String topic = "vm-topic-1";
        properties.put("group.id", "flink-group-1");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StateBackend stateBackend = new FsStateBackend("hdfs:///flink/checkpoint/DEMO", true);
        env.setStateBackend(stateBackend);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.enableCheckpointing(120000);

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topic
                , new SimpleStringSchema()
                , properties);

        //添加数据源
        DataStream<String> kafkaSource = env.addSource(consumer)
                .name("kafkaS数据接入");
        kafkaSource.print();
        env.execute("简单输出kafka");
    }
}
