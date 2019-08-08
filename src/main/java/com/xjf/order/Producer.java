package com.xjf.order;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * 顺序消息，生产者
 *
 * @author xjf
 * @date 2019/7/31 9:29
 */
public class Producer {

    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        String producerGroup = "order_producer";

        DefaultMQProducer producer = new DefaultMQProducer(producerGroup);
        producer.setNamesrvAddr("139.224.129.156:9876");
        producer.start();

        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String time = sdf.format(date);

        //使用三个队列来分别发送5条消息，在每个队列中的消息是被顺序消费的，但是队列与队列之间的顺序是不确定的

        //发送5条顺序消息，队列1
        for (int i = 0; i < 5; i++) {
            String msgBody = time + "; Hello RocketMQ " + i + "; Queue:" + 1;
            Message msg = new Message("TopicOrder","TagA","KEY",msgBody.getBytes());

            //此处为设置消息顺序发送
            //list为队列的list，o为指定的队列
            SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                    Integer index = (Integer)o;
                    System.out.println("index: " + index);

                    return list.get(index);
                }
            }, 1);

            System.out.println(sendResult);
        }

        //发送5条顺序消息，队列2
        for (int i = 0; i < 5; i++) {
            String msgBody = time + "; Hello RocketMQ " + i + "; Queue:" + 2;
            Message msg = new Message("TopicOrder","TagA","KEY",msgBody.getBytes());

            //此处为设置消息顺序发送
            //list为队列的list，o为指定的队列
            SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                    Integer index = (Integer)o;
                    System.out.println("index: " + index);

                    return list.get(index);
                }
            }, 2);

            System.out.println(sendResult);
        }

        //发送5条顺序消息，队列3
        for (int i = 0; i < 5; i++) {
            String msgBody = time + "; Hello RocketMQ " + i + "; Queue:" + 3;
            Message msg = new Message("TopicOrder","TagA","KEY",msgBody.getBytes());

            //此处为设置消息顺序发送
            //list为队列的list，o为指定的队列
            SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                    Integer index = (Integer)o;
                    System.out.println("index: " + index);

                    return list.get(index);
                }
            }, 3);

            System.out.println(sendResult);
        }

        //关闭生产者
        producer.shutdown();
    }
}
