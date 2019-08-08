package com.xjf.pull;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * 消费者使用pull拉取消息，生产者
 *
 * @author xjf
 * @date 2019/8/1 18:28
 */
public class Producer {

    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        String groupName = "pull_producer";
        DefaultMQProducer producer = new DefaultMQProducer(groupName);

        producer.setNamesrvAddr("139.224.129.156:9876");
        producer.start();

        for (int i = 0; i < 20; i++) {
            Message message = new Message("TopicPull","TagA","key",("Hello XJF" + i).getBytes());
            SendResult sendResult = producer.send(message);
            System.out.println(sendResult);
//            Thread.sleep(1000);
        }

        producer.shutdown();
    }
}
