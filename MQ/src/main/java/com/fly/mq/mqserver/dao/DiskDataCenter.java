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
public class DiskDataCenter {
    // 数据库
    private MassageFileManager massageFileManager = new MassageFileManager();
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
    public void insertQueue(MSGQueue queue){
        dataBaseManager.insertQueue(queue);
    }

    public void deleteQueue(String queueName){
        dataBaseManager.deleteQueue(queueName);
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
        massageFileManager.sendMessage(queue,message);
    }

    public void deleteMessage(MSGQueue queue, Message message) throws IOException, ClassNotFoundException {
        massageFileManager.deleteMessage(queue,message);
        if(massageFileManager.checkGC(queue.getName())){
            massageFileManager.gc(queue);
        };
    }

    public LinkedList<Message> loadAllMessageFromQueue(String queueName) throws IOException, ClassNotFoundException {
        return massageFileManager.loadAllMessageFromQueue(queueName);
    }

}
