package com.xjf.order;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 顺序消息，消费者
 *
 * @author xjf
 * @date 2019/7/31 9:40
 */
public class Customer {

    public static void main(String[] args) throws MQClientException {
        String customerGroup = "order_customer";
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(customerGroup);

        consumer.setNamesrvAddr("139.224.129.156:9876");

        //不管是从第一个消费还是从最后一个开始消费，因为是顺序消费，它的消费顺序是一致的
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe("TopicOrder","TagA");

        consumer.registerMessageListener(new Listener());

        consumer.start();
        System.out.println("consumer started.");
    }
}

/**
 * 有序消费消息
 */
class Listener implements MessageListenerOrderly{

    private Random random = new Random();

    public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {
        //设置自动提交
        consumeOrderlyContext.setAutoCommit(true);

        for (MessageExt messageExt : list) {
            System.out.println(messageExt + ", content:" + new String(messageExt.getBody()));
        }

        try {
            //模拟业务逻辑处理中...
            TimeUnit.SECONDS.sleep(random.nextInt(5));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return ConsumeOrderlyStatus.SUCCESS;
    }
}