package com.xjf.model;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

/**
 * 消费者1
 *
 * @author xjf
 * @date 2019/8/3 18:39
 */
public class Consumer1 {

    public static void main(String[] args) throws MQClientException {
        String groupName = "model_consumer";
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(groupName);

        consumer.setNamesrvAddr("139.224.129.156:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe("TopicModel","*");
        //设置消费者的消费模式
        consumer.setMessageModel(MessageModel.BROADCASTING);
        consumer.setMessageListener(new Listener());

        consumer.start();
        System.out.println("Consumer1 start...");
    }
}

class Listener implements MessageListenerConcurrently{

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {

        try {
            MessageExt msg = msgs.get(0);

            String topic = msg.getTopic();
            String body = new String(msg.getBody());
            String tags = msg.getTags();
            System.out.println("收到消息：topic=" + topic +
                    " msg=" + body +
                    " tags=" + tags);
        } catch (Exception e) {
            e.printStackTrace();

            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        }

        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }
}
