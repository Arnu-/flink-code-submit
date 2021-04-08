package me.arnu.flink.demo;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Main {
    public static void main(String[] args) throws Exception {
        Logger logger = LoggerFactory.getLogger("Main");
        logger.info("记录一下");
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
        kafkaSource.addSink(new SinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) throws Exception {
                SinkFunction.super.invoke(value, context);
                logger.info("记录一下:" + value);
                System.out.println(value);
            }
        }).name("输出");
//                .print();
        env.execute("简单输出kafka");
    }
}
