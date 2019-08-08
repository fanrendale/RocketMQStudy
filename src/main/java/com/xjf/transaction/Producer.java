package com.xjf.transaction;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionCheckListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.concurrent.TimeUnit;

/**
 * 事务消息，生产者
 *
 * @author xjf
 * @date 2019/7/31 17:42
 */
public class Producer {

    public static void main(String[] args) throws MQClientException, InterruptedException {
        /**
         * 一个应用创建一个Producer，由应用来维护此对象，可以设置为全局对象或者单例
         * 注意：ProducerGroupName需要由应用来保证唯一，一类Producer集合的名称，这类Producer通常发送一类消息，且发送逻辑一致
         * ProducerGroup这个概念发送普通的消息时，作用不大，但是发送分布式事务消息时，比较关键。
         * 因为服务器会回查这个Group下的任意一个Producer
         */
        String groupName = "transaction_producer";
        final TransactionMQProducer producer = new TransactionMQProducer(groupName);

        //nameserver服务
        producer.setNamesrvAddr("139.224.129.156:9876");
        //事务回查最小并发数
        producer.setCheckThreadPoolMinSize(5);
        //事务回查最大并发数
        producer.setCheckThreadPoolMaxSize(20);
        //队列数
        producer.setCheckRequestHoldMax(2000);

        //服务器回调Producer，检查本地事务分支成功还是失败.就是当生产者的第二次确认消息没发送或者发送为UNKNOW时
        producer.setTransactionCheckListener(new TransactionCheckListener() {
            public LocalTransactionState checkLocalTransactionState(MessageExt messageExt) {
                System.out.println("state:" + new String(messageExt.getBody()));

                //if数据入库真实发生变化，则再次提交状态
                //else数据库没有发生变化，则直接忽略该数据回滚即可
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        });

        /**
         * Producer对象在使用之前必须要调用start初始化，初始化一次即可
         * 注意：切记不可以在每次发送消息时，都调用start方法
         */
        producer.start();

        /**
         * 下面这段代码表明一个Producer对象可以发送多个topic，多个tag的消息。
         * 注意：send方法是同步调用，只要不抛异常就标识成功。但是发送成功也可会有多种状态。
         * 例如消息写入Master成功，但是Slave不成功，这种情况消息属于成功，但是对于个别应用如果对消息可靠性要求极高，
         * 需要对这种情况做处理。另外，消息可能会存在发送失败的情况，失败重试由应用来处理。
         */
        TransactionExecuterImpl tranExecuter = new TransactionExecuterImpl();

        for (int i = 0; i < 1; i++) {
            //key消息关键词，多个key用MessageConst.KEY_SEPARATOR隔开（查询消息使用）
            Message msg = new Message("TopicTransaction","Transaction" + i, "key",("Hello Dale " + i).getBytes());
            SendResult sendResult = producer.sendMessageInTransaction(msg,tranExecuter,"tq");
            System.out.println(sendResult);

            TimeUnit.SECONDS.sleep(1000);
        }

        /**
         * 应用退出时，要调用shutdown来清理资源，关闭网络连接，从MetaQ服务器上注销自己
         * 注意：我们建议应用在JBOSS、Tomcat等容器的退出钩子里调用shutdown方法
         */
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                producer.shutdown();
            }
        }));

        System.exit(0);
    }
}
