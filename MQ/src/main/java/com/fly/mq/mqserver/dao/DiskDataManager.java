package com.fly.mq.mqserver.dao;

import com.fly.mq.mqserver.core.Binding;
import com.fly.mq.mqserver.core.Exchange;
import com.fly.mq.mqserver.core.MSGQueue;
import com.fly.mq.mqserver.core.Message;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * 统一管理 硬盘上的文件
 * 数据库：交换机，队列，绑定
 * 数据文件：消息
 */
public class DiskDataManager {
    // 数据库
    private MessageFileManager messageFileManager = new MessageFileManager();
    // 数据文件
    private DataBaseManager dataBaseManager = new DataBaseManager();

    public void init(){
        dataBaseManager.init();
    }

    // 封装交换机操作
    public void insertExchange(Exchange exchange){
        dataBaseManager.insertExchange(exchange);
    }

    public void deleteExchange(String exchangeName){
        dataBaseManager.deleteExchange(exchangeName);
    }

    public List<Exchange> getAllExchanges(){
        return dataBaseManager.getExchangeList();
    }
    // 队列
    public void insertQueue(MSGQueue queue) throws IOException {
        // 创建队列的同时还需要创建出队列的目录文件
        dataBaseManager.insertQueue(queue);
        messageFileManager.createQueueFiles(queue.getName());
    }

    public void deleteQueue(String queueName) throws IOException {
        // 删除队列的同时删除队列对应的目录和文件
        dataBaseManager.deleteQueue(queueName);
        messageFileManager.destroyQueueFiles(queueName);
    }

    public List<MSGQueue> getAllQueues(){
        return dataBaseManager.getQueueList();
    }

    // 绑定
    public void insertBinding(Binding binding){
        dataBaseManager.insertBinding(binding);
    }

    public void deleteBinding(Binding binding){
        dataBaseManager.deleteBinding(binding);
    }

    public List<Binding> getAllBindings(){
        return dataBaseManager.getBindingList();
    }

    // 消息
    public void sendMessage(MSGQueue queue, Message message) throws IOException {
        messageFileManager.sendMessage(queue,message);
    }

    public void deleteMessage(MSGQueue queue, Message message) throws IOException, ClassNotFoundException {
        messageFileManager.deleteMessage(queue,message);
        if(messageFileManager.checkGC(queue.getName())){
            messageFileManager.gc(queue);
        };
    }

    public LinkedList<Message> loadAllMessageFromQueue(String queueName) throws IOException, ClassNotFoundException {
        return messageFileManager.loadAllMessageFromQueue(queueName);
    }

}
