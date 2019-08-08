package com.xjf.quickstart;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * 模拟消费者宕机，
 *
 * 启动第一个消费者和当前的第二个消费者，生产者生产一条消息，如果是第一个消费者先获取到，然后消费者1会先睡眠30s
 * ，在睡眠时将消费者1断开连接。则消费者2会马上进行消费，这是因为broker在消费者1没有消费时，没有收到返回的确认
 * 消费的提示，则马上会把消息进行重试投递到同一主题的同一个Group的消费者，然后能进行正常消费。
 *
 * @author xjf
 * @date 2019/7/30 16:42
 */
public class Customer2 {

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

                    //模拟消费失败，出异常.会触发重试
//                    int a = 1 / 0;
                } catch (Exception e) {
                    e.printStackTrace();
                    System.err.println("消费失败，稍后重试...");

                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }

                //response broker(ACK)
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();
        System.out.println("Consumer2 Started.");
    }
}
