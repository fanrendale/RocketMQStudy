package com.xjf.filter;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * @author xjf
 * @date 2019/8/4 11:17
 */
public class Consumer {

    public static void main(String[] args) throws MQClientException {
        String groupName = "filter_consumer";
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(groupName);

        consumer.setNamesrvAddr("139.224.129.156:9876");
        //使用sql来进行过滤
        consumer.subscribe("TopicFilter",MessageSelector.bySql(
                "TAGS is not null and TAGS in ('TagB','TagC') " +
                " and index is not null and (index between 5 and 9)"));
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                MessageExt msg = msgs.get(0);

                try {
                    String topic = msg.getTopic();
                    String content = new String(msg.getBody(), RemotingHelper.DEFAULT_CHARSET);
                    String tag = msg.getTags();
                    String index = msg.getUserProperty("index");

                    System.out.println("收到消息：topic:" + topic + ", content:" + content +
                            ", tag:" + tag + ", index:" + index);

                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();

                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();
        System.out.println("filter consumer started...");
    }
}
