package com.xjf.filter;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;

/**
 * 消息过滤
 * 生产者
 *
 * @author xjf
 * @date 2019/8/4 11:06
 */
public class Producer {

    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException, MQBrokerException {
        String groupName = "filter_producer";
        DefaultMQProducer producer = new DefaultMQProducer(groupName);

        producer.setNamesrvAddr("139.224.129.156:9876");
        producer.start();

        for (int i = 0; i < 10; i++) {
            String tag = "";
            switch (i % 3){
                case 0:{
                    tag = "TagA";
                    break;
                }
                case 1:{
                    tag = "TagB";
                    break;
                }
                case 2:{
                    tag = "TagC";
                    break;
                }
                default:{
                    tag = "Tag";
                    break;
                }
            }

            Message message = new Message("TopicFilter",tag,"key",
                    ("Hello XJF" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            //存放自定义的属性，可以在过滤时用作条件
            message.putUserProperty("index",String.valueOf(i));

            //发送消息
            SendResult sendResult = producer.send(message);
            System.out.println(sendResult);
        }

        producer.shutdown();
    }
}
