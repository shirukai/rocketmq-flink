package org.apache.rocketmq.flink.example;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.flink.legacy.RocketMQConfig;
import org.apache.rocketmq.flink.legacy.RocketMQSink;

import java.util.Properties;

/**
 * 通过Message设置用户属性的示例
 *
 * 本地监听19001端口 nc -lk 19001
 * 发送数据格式 1,xiaoming,18
 * @author shirukai
 */
public class SetUserPropertyExamples {
    private static final String ROCKETMQ_PRODUCER_TOPIC = "flink-sink-test";

    public static void main(String[] args) throws Exception {
        // 创建Flink运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 忽略env的配置

        // 从Socket中监听消息
        DataStreamSource<String> source = env.socketTextStream("localhost", 19001);


        // 处理数据，并生成RocketMQ的Message
        DataStream<Message> res = source.process(new ProcessFunction<String, Message>() {
            @Override
            public void processElement(String s, ProcessFunction<String, Message>.Context context, Collector<Message> collector) throws Exception {
                String[] items = s.split(",");
                String keys = items[0];
                String name = items[1];
                String age = items[2];
                Message message = new Message(ROCKETMQ_PRODUCER_TOPIC, "*", keys, name.getBytes());
                message.putUserProperty("age", age);
                collector.collect(message);
            }
        });

        Properties producerProps = new Properties();
        producerProps.setProperty(RocketMQConfig.NAME_SERVER_ADDR, "localhost:9876");
        res.addSink(new RocketMQSink(producerProps));

        env.execute("SetUserPropertyExamples");
    }
}
