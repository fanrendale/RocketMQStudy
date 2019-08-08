package com.xjf.transaction;

import org.apache.rocketmq.client.producer.LocalTransactionExecuter;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.common.message.Message;

/**
 * 执行本地事务，由客户端回调
 * @author xjf
 * @date 2019/7/31 17:57
 */
public class TransactionExecuterImpl implements LocalTransactionExecuter {
    @Override
    public LocalTransactionState executeLocalTransactionBranch(Message message, Object o) {
        System.out.println(message.toString());
        System.out.println("message = " + new String(message.getBody()));
        System.out.println("arg = " + o);
        String tag = message.getTags();

        System.out.println("这里执行入库操作...入库成功...");

        /*if ("Transaction3".equals(tag)){
            //这里有一个分阶段提交任务的概念
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }*/

        /*try {
            System.out.println("sleep start");
            Thread.sleep(1000000);
            System.out.println("sleep end");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/

        //如果执行的业务操作成功，但是没有返回标识信息位置，则认为第二次没有发出
        //第二次确认消息
        return LocalTransactionState.COMMIT_MESSAGE;
    }
}
