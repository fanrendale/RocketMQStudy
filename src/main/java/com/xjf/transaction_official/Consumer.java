package com.xjf.transaction_official;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * 事务消息，消费者
 *
 * @author xjf
 * @date 2019/7/31 18:04
 */
public class Consumer {

    public static void main(String[] args) throws MQClientException {
        String group_name = "transaction_consumer";
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group_name);

        consumer.setNamesrvAddr("139.224.129.156:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe("TopicTest1234","*");

        consumer.registerMessageListener(new Listener());

        consumer.start();
        System.out.println("transaction official consumer started.");
    }
}

class Listener implements MessageListenerConcurrently{
    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
        MessageExt msg = list.get(0);

        try {
            String topic = msg.getTopic();
            String body = new String(msg.getBody(), RemotingHelper.DEFAULT_CHARSET);
            String tag = msg.getTags();

            System.out.println("收到消息：topic=" + topic + ", body=" + body + ", tag=" + tag);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();

            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        }


        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }
}
