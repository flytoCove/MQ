package com.fly.mq.mqserver.core;

import com.fly.mq.common.Consumer;
import com.fly.mq.common.ConsumerEnv;
import com.fly.mq.common.MQException;
import com.fly.mq.mqserver.VirtualHost;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 实现消费消息的核心逻辑
 */
public class ConsumerManager {
    // 持有 VirtualHost 的引用用来操作数据
    private VirtualHost parent;
    // 指定一个线程池 用来执行指定的任务
    ExecutorService workerPool = Executors.newFixedThreadPool(4);
    // 存放令牌的队列（队列名）
    private BlockingQueue<String> tokenQueue = new LinkedBlockingQueue<>();
    // 扫描线程
    private Thread scannerThread = null;

    public ConsumerManager(VirtualHost parent) {
        this.parent = parent;
        scannerThread = new Thread(() -> {
            while(true) {
                try {
                    // 1.拿到有消息的令牌（队列名）
                    String queueName = tokenQueue.take();
                    // 2.根据令牌查找队列名
                    MSGQueue queue = parent.getMemoryDataManager().getQueue(queueName);
                    if(queue==null) {
                        throw new MQException("[ConsumerManager] Queue not found queueName: " + queueName);
                    }
                    // 3.从这个队列中消费消息
                    synchronized(queue) {
                        consumeMessage(queue);
                    }
                } catch (InterruptedException | MQException e) {
                    e.printStackTrace();
                }
            }
        });
        // 设为守护线程
        scannerThread.setDaemon(true);
        scannerThread.start();

    }

    // 发送消息的时候调用
    public void notifyConsume(String queueName) throws InterruptedException {
        tokenQueue.put(queueName);
    }

    //
    public void addConsumer(String consumerTag, String queueName, boolean autoAck, Consumer consumer) {
        // 找到对应的队列
        MSGQueue queue = parent.getMemoryDataManager().getQueue(queueName);
        if(queue == null){
            throw new MQException("[ConsumerManager] Queue not found queueName = " + queueName);
        }
        ConsumerEnv consumerEnv = new ConsumerEnv(consumerTag, queueName, autoAck, consumer);
        synchronized (queue){
            queue.addConsumerEnv(consumerEnv);
            // 此时如果队列中有消息 需要立即消费
            int n = parent.getMemoryDataManager().getMessageCount(queueName);
            for(int i = 0; i < n; i++){
                // 消费一条消息
                consumeMessage(queue);
            }
        }
    }

    /**
     * 消费消息
     * @param queue
     */
    private void consumeMessage(MSGQueue queue) {
        // 1.按照轮询的方式找到一个消费者
        ConsumerEnv luckyOne = queue.chooseConsumerEnv();
        if(luckyOne == null) {
            // 如果没有的消费者
            return;
        }
        Message message = parent.getMemoryDataManager().pollMessage(queue.getName());
        if(message == null){
            // 当前队列没有消息
            return;
        }

        // 把消息带到消费者的回调方法中丢给线程池执行
        workerPool.submit(() -> {
            try {
                // 1.把消息放到待确认集合中
                parent.getMemoryDataManager().addMessageWaitAck(queue.getName(), message);
                // 2.执行回调
                try{
                    luckyOne.getConsumer().handleDelivery(luckyOne.getConsumerTag(), message.getBasicProperties(), message.getBody());
                }catch (Exception e){
                    System.out.println("[ConsumerManager] Consumer handleDelivery threw exception");
                    e.printStackTrace();
                    return; // 提前退出，不执行后续删除逻辑
                }
                // 3.如果 autoAck == true 直接删掉 如果为 false 则需要后续消费者调用 basicAck()
                if (luckyOne.isAutoAck()) {
                    // 1) 删除硬盘上的消息
                    if(message.getDeliverMode() == 2) {
                        parent.getDiskDataManager().deleteMessage(queue, message);
                    }
                    // 2) 删除待确认集合
                    parent.getMemoryDataManager().removeMessageWaitAck(queue.getName(),message.getMessageId());
                    // 3) 内存中删除消息中心的数据
                    parent.getMemoryDataManager().removeMessage(message.getMessageId());

                    System.out.println("[ConsumerManager] message is consumed " + luckyOne.getConsumerTag());
                }
            }catch (Exception e) {
                System.out.println("[ConsumerManager] Consume message failed");
                e.printStackTrace();
            }
        });
    }



}
