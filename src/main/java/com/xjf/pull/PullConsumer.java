package com.xjf.pull;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * 拉取消息消费，消费者
 * 队列的偏移量存储在数据库中，每次拉取前从数据库中取偏移量，拉取后将当前偏移量存到数据库中
 *
 * @author xjf
 * @date 2019/8/1 21:42
 */
public class PullConsumer {
    /**
     * 记录队列的偏移量，key为队列，value为偏移量。本来应该存储到数据库中，此处用内存做模拟
     */
    private static HashMap<MessageQueue,Long> offsetTable = new HashMap<>();

    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        String groupName = "pull_consumer";
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(groupName);

        /**
         * Pull Consumer配置：
         *
         * brokerSuspendMaxTimeMillis 20000 长轮询，Consumer拉消息请求在Broker挂起最长时间，单位毫秒
         * consumerPullTimeoutMillis 10000 非长轮询，拉消息超时时间，单位毫秒
         * consumerTimeoutMillisWhenSuspend 30000 长轮询，Consumer拉消息请求在broker挂起超过指定时间，客户端任务超时，单位毫秒
         * messageModel BROADCASTING 消息模型，支持以下两种：1、集群消费 2、广播模式
         * messageQueueListener 监听队列变化
         * offsetStore 消费进度存储
         * registerTopics 注册的topic集合
         * allocateMessageQueueStrategy Rebalance算法实现策略
         */

        consumer.setNamesrvAddr("139.224.129.156:9876");
        consumer.start();

        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


        //根据主题获取所有的队列，默认4条队列
        Set<MessageQueue> queueSet = consumer.fetchSubscribeMessageQueues("TopicPull");

        //遍历每一个队列，进行拉取数据
        for (MessageQueue queue : queueSet) {
            System.out.println("Consume from the queue: " + queue);

            SINGLE_MQ:while (true){
                //从queue中获取数据，从什么位置开始拉取数据，单次最多拉取2条记录
                //如果没有消息，则阻塞10s
                PullResult pullResult = consumer.pullBlockIfNotFound(queue, null, getMessageQueueOffset(queue), 2);
                System.out.println(pullResult);
                System.out.println(pullResult.getPullStatus());
                System.out.println();
                putMessageQueueOffset(queue,pullResult.getNextBeginOffset());
                switch (pullResult.getPullStatus()){
                    case FOUND:{
                        //消费数据
                        List<MessageExt> list = pullResult.getMsgFoundList();

                        System.out.println("队列id:" + queue.getQueueId() + "-----------------------------start-------------------------");
                        for (MessageExt msg : list) {
                            String time = sdf.format(new Date(System.currentTimeMillis()));
                            System.out.println("队列id:" + queue.getQueueId() + " time:" + time +
                                    " msg:" + new String(msg.getBody()));
                        }
                        System.out.println("队列id:" + queue.getQueueId() + "-----------------------------end-------------------------");

                        Thread.sleep(5000);
                        break ;
                    }
                    case OFFSET_ILLEGAL:
                        break ;
                    case NO_NEW_MSG:
                        System.out.println("没有新消息啦。。。");
                        break SINGLE_MQ;
                    case NO_MATCHED_MSG:
                        break ;
                    default:
                        break ;
                }
            }
        }

        consumer.shutdown();
    }

    /**
     * 将消费后的偏移量存入数据库
     * @param queue
     * @param nextBeginOffset
     */
    private static void putMessageQueueOffset(MessageQueue queue, long nextBeginOffset) {
       offsetTable.put(queue,nextBeginOffset);
    }

    /**
     * 获取指定队列的偏移量
     * @param queue
     * @return
     */
    private static long getMessageQueueOffset(MessageQueue queue) {
        Long offset = offsetTable.get(queue);
        if (offset != null){
            return offset;
        }

        return 0;
    }
}
