package com.xjf.quickstart;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 普通消息
 * 消费者，消费消息
 *
 * @author xjf
 * @date 2019/7/30 16:42
 */
public class Customer {

    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("quickstart_consumer");

        /**
         * Push Consumer配置：
         *
         * messageModel CLUSTERING 消息模型，支持以下两种：1.集群消费 2.广播消费
         * consumeFromWhere CONSUME_FROM_LAST_OFFSET Consumer启动后，默认从什么位置开始消费
         * allocateMessageQueueStrategy
         * AllocateMessageQueueAveragely Rebalance算法实现策略
         * Subscription 订阅关系
         * messageListener 消息监听器
         * offsetStore 消费进度存储
         * consumeThreadMin 10 消费线程池数量
         * consumeThreadMax 20 消费线程池数量
         * consumeConcurrentlyMaxSpan 2000 单队列并行消费允许的最大跨度
         * pullThresholdForQueue 1000 拉消息本地队列缓存消息最大数
         * pullInterval 拉消息间隔，由于是长轮询，所以为0，但是如果应用了流控，也可以设置大于0的值，单位毫秒
         * consumeMessageBatchMaxSize 1 批量消费，一次消费多少条消息
         * pullBatchSize 32 批量拉消息，一次最多拉多少条
         */

        consumer.setNamesrvAddr("139.224.129.156:9876");
        /**
         * 设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费
         * 如果非第一次启动，那么按照上次消费的位置继续消费
         */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);

        //消费者订阅主题
        consumer.subscribe("TopicQuickStart","*");

        //注册消息监听器
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
//                System.out.println("条数：" + list.size());

                MessageExt msg = list.get(0);

                try {
                    String topic = msg.getTopic();
                    String msgBody = new String(msg.getBody(),"utf-8");
                    String tags = msg.getTags();

                    /**
                     * 发送10条消息，模拟消息实体内容为5的这条消息在消费端消费失败（抛异常，MQ进行消息重发操作）
                     * 这种情况，10条消息都会被重新发送过来，会有问题的
                     * 所以如果是需要批量消费，则需要特殊编写业务逻辑
                     */

                    System.out.println("收到消息：" + " topic:" + topic + " ,tags: " + tags + ", msg: " + msgBody);

                    //获取消息的原始id，当正常消费时，broker会返回消息和一个消息的id，不过对于同一个消息
                    //在多次返回时的id是不同的。
                    //不过如果是重试时进行返回，则会有新生成的id和该消息第一次返回的的id（称为原始id），
                    // 第一次这个变量的值会是空的。
                    String originMsgId = msg.getProperty(MessageConst.PROPERTY_ORIGIN_MESSAGE_ID);
                    System.out.println("originMsgId: " + originMsgId);

                    //模拟消费失败，出异常.会触发重试
                    int a = 1 / 0;

                    //睡眠30s，模拟消费者宕机，没有返回broker确认消费完成的信息，和Customer2类联合测试
//                    TimeUnit.SECONDS.sleep(30);


                } catch (Exception e) {
                    e.printStackTrace();

                    //获取消息的重试次数（正常broker第一次将消息发过来时，不算为重试次数，进入异常处理之后再发回来消息才开始算），
                    // 如果次数等于3次，则将该消息存入数据库做记录，便于之后人工处理这条消息（人工补偿）
                    if (msg.getReconsumeTimes() == 3){
                        System.err.println("-----------记录日志，存入数据库------------");

                        //通知broker正常消费了
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    }

                    System.err.println("消费失败，稍后重试...");

                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }

                //response broker(ACK)
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();
        System.out.println("Consumer Started.");
    }
}
