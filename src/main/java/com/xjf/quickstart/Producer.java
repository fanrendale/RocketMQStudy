package com.xjf.quickstart;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * 普通消息
 * 生产者，发送消息
 *
 * 消息分类：
 * 1.NormalProducer（普通）：使用传统的send发送即可。这种模式下不能保证消息的顺序一致性
 * 2.OrderProducer（顺序）：rocketmq可以保证严格的消息顺序进行消费。
 *          遵循全局顺序的时候使用一个queue，局部顺序的时候可以多个queue并行消费。
 * 3.TransactionProducer（事务）：支持事务方式对消息进行提交处理，在rocket里事务分为两个阶段。
 *          第一个阶段为把消息传递给MQ，只不过消费端不可见，但是数据其实已经发送到broker了。
 *          第二个阶段为本地消息回调处理，如果成功的话返回COMMIT_MESSAGE,则在broker上的数据对
 *          消费端可见，失败则为ROLLBACK_MESSAGE,消费端不可见。
 *
 * @author xjf
 * @date 2019/7/30 16:19
 */
public class Producer {

    public static void main(String[] args) throws MQClientException {
        DefaultMQProducer producer = new DefaultMQProducer("quickstart_producer");

        /**
         * producer配置项：
         *
         * producerGroup DEFAULT_PRODUCER
         * producer组名，多个Producer如果属于一个应用，发送同样的消息，则应该将它们归为同一组。
         * createTopicKey TBW在发送消息时，自动创建服务器不存在的topic，需要制定key
         * defaultTopicQueueNums 4 在发送消息时，自动创建服务器不存在的topic，默认创建的队列数
         ** sendMsgTimeout 10000 发送消息超时时间，单位毫秒
         * compressMsgBodyOverHowmuch 4096 消息Body超过多大开始压缩（Consumer收到消息会自动解压缩），单位字节
         ** retryTimesWhenSendFailed 重试次数（可以配置）
         ** retryAnotherBrokerWhenNotStoreOK FALSE 如果发送消息返回sendResult,但是sendStatus != Send_OK,是否重试发送
         ** maxMessageSize 131072 客户端限制的消息大小，超过报错，同时服务端也会限制（默认128k）
         * transactionCheckListener 事务消息回查监听器，如果发送事务消息，必须设置
         * checkThreadPoolMinSize 1 Broker回查Producer事务状态时，线程池大小
         * checkThreadPoolMaxSize 1 Broker回查Producer事务状态时，线程池大小
         * checkRequestHoldMax 2000 Broker回查Producer事务状态时，Producer本地缓冲请求队列大小
         */

        producer.setRetryTimesWhenSendFailed(10);
        producer.setMaxMessageSize(1024);

        producer.setNamesrvAddr("139.224.129.156:9876");
        producer.start();

        /*byte[] bytes = new byte[1024];

        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte)i;
        }

        try {
            Message msg = new Message("TopicQuickStart","TagA","kkk",bytes);
            System.out.println(msg.toString());

            //设置发送超时时间为10s
            SendResult sendResult = producer.send(msg,10000);
            System.out.println(sendResult);
        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (MQBrokerException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/

        //发送10条消息，模拟消息实体内容为5的这条消息在消费端消费失败（抛异常，MQ进行消息重发操作）
        for (int i = 0; i < 1; i++) {
            try {
                Message msg = new Message("TopicQuickStart", "TagA","kkk",("Hello RocketMQ " + i).getBytes());
                System.out.println(msg.toString());
                SendResult sendResult = producer.send(msg);
                System.out.println(sendResult);
//                Thread.sleep(1000);
            } catch (RemotingException e) {
                e.printStackTrace();
            } catch (MQBrokerException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        producer.shutdown();
    }
}
