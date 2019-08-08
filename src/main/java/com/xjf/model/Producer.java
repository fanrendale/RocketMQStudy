package com.xjf.model;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * 测试不同的消费模式：
 * 1、集群模式：多个消费者对消息进行负载均衡的消费，所有消费者加起来的消息等于总消息
 * 2、广播模式：所有的消费者都消费所有的相同的消息
 *
 * @author xjf
 * @date 2019/8/3 18:34
 */
public class Producer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        String groupName = "model_producer";
        DefaultMQProducer producer = new DefaultMQProducer(groupName);

        producer.setNamesrvAddr("139.224.129.156:9876");
        producer.start();

        for (int i = 0; i < 10; i++) {
            Message message = new Message("TopicModel","Tag","key",
                    ("消息内容" + i).getBytes());
            SendResult sendResult = producer.send(message);
            System.out.println(sendResult);
        }

        producer.shutdown();
    }
}
