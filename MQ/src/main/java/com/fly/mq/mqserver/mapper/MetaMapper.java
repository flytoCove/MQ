package com.fly.mq.mqserver.mapper;

import com.fly.mq.mqserver.core.Binding;
import com.fly.mq.mqserver.core.Exchange;
import com.fly.mq.mqserver.core.MSGQueue;
import org.apache.ibatis.annotations.Mapper;

import java.util.List;

@Mapper
public interface MetaMapper {
    // 提供三个核心建表方法
    void createExchangeTable();
    void createQueueTable();
    void createBindingTable();

    //
    void insertExchange(Exchange exchange);
    List<Exchange> getExchangeList();
    void deleteExchange(String exchangeName);
    void insertQueue(MSGQueue queue);
    List<MSGQueue> getQueueList();
    void deleteQueue(String queueName);
    void insertBinding(Binding binding);
    List<Binding> getBindingList();
    // 没有设置主键所以这里通过 exchangeName 和 queueName 两个维度进行操作
    void deleteBinding(Binding binding);

}
