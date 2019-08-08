package com.xjf.pull;

import org.apache.rocketmq.client.consumer.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * 消费者拉消息，消费者
 * 消息消费后的偏移量储存在Broker中，下次拉取时也从Broker中获取当前偏移量
 *
 * 此种方法会有重复消费的情况，拉取消息的间隔时间和偏移量更新到broker的间隔时间不是同步的，所以会重复消费。需要自己做去重操作。
 * 在一个consumer中将拉取消息的间隔设置大于偏移量更新间隔，可以做到不重复消费。在生产环境中多个消费者，不管怎么设置都会有重复消费存在
 *
 * @author xjf
 * @date 2019/8/1 20:10
 */
public class PullScheduleService {

    public static void main(String[] args) throws MQClientException {
        String groupName = "pull_consumer";
        final MQPullConsumerScheduleService scheduleService = new MQPullConsumerScheduleService(groupName);

        scheduleService.getDefaultMQPullConsumer().setNamesrvAddr("139.224.129.156:9876");
        scheduleService.setMessageModel(MessageModel.CLUSTERING);

        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        scheduleService.registerPullTaskCallback("TopicPull", new PullTaskCallback() {
            @Override
            public void doPullTask(MessageQueue messageQueue, PullTaskContext pullTaskContext) {

                MQPullConsumer consumer = pullTaskContext.getPullConsumer();

                try {
                    //获取从哪里获取，即获取队列的偏移量
                    long offset = consumer.fetchConsumeOffset(messageQueue,false);
                    if (offset < 0){
                        offset = 0;
                    }

                    //最后一个参数为从每个队列一次拉取的最大消息数
                    PullResult pullResult = consumer.pull(messageQueue,"*",offset,2);

                    switch (pullResult.getPullStatus()){
                        case FOUND:{
                            List<MessageExt> list = pullResult.getMsgFoundList();
                            for (MessageExt msg : list) {
                                //消费数据
                                System.out.println("队列id:" + messageQueue.getQueueId() + "-----------------------------start-------------------------");
                                String time = sdf.format(new Date(System.currentTimeMillis()));
                                System.out.println("队列id:" + messageQueue.getQueueId() + " time:" + time +
                                        " msg:" + new String(msg.getBody()));
                                System.out.println("队列id:" + messageQueue.getQueueId() + "-----------------------------end-------------------------");

                            }
                            break;
                        }
                        case NO_MATCHED_MSG:
                            break;
                        case NO_NEW_MSG:
                            System.out.println("没有消息啦");
                            break;
                        case OFFSET_ILLEGAL:
                            break;
                        default:
                            break;
                    }

                    //存储offset，客户端每隔5s会定时刷新到Broker
                    consumer.updateConsumeOffset(messageQueue,pullResult.getNextBeginOffset());
                    //设置再过3000ms后重新拉取消息
                    pullTaskContext.setPullNextDelayTimeMillis(200);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        scheduleService.start();
    }
}
