package org.apache.rocketmq.flink.example;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;


/**
 * 根据属性过滤的示例
 *
 * @author shirukai
 */
public class PropertyFilterExamples {
    private static final String ROCKETMQ_PRODUCER_TOPIC = "rocketmq_sink_with_dynamic_property";

    public static void main(String[] args) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("property-filter-examples-1");
        // 指定从头消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setNamesrvAddr("localhost:9876");
        consumer.subscribe(ROCKETMQ_PRODUCER_TOPIC,MessageSelector.bySql("item = 'iphone 12'"));
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            msgs.forEach(msg -> {
                System.out.println("消费消息：body=" + new String(msg.getBody())+",user="+msg.getUserProperty("user")+", item="+msg.getUserProperty("item"));
            });
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
        Thread.currentThread().join();
    }
}
