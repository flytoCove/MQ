package com.fly.mq.mqserver.dao;

import com.fly.mq.MqApplication;
import com.fly.mq.mqserver.core.Binding;
import com.fly.mq.mqserver.core.Exchange;
import com.fly.mq.mqserver.core.ExchangeType;
import com.fly.mq.mqserver.core.MSGQueue;
import com.fly.mq.mqserver.mapper.MetaMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.File;
import java.util.List;

/**
 * 通过这个类整合数据库操作
 */
@Slf4j
public class DataBaseManager {
    private MetaMapper metaMapper;

    // 初始化操作
    public void init(){

        // 这里使用 @Autowired 无效，因为 DataBaseManager 并没有交给 Spring 管理
        //Spring 不会扫描和处理非托管类中的依赖注入
        // 手动从 Spring 容器获取依赖 metaMapper
        metaMapper = MqApplication.context.getBean(MetaMapper.class);

        if(!checkDBExists()){
            File dataDir = new File("./data");
            if(!dataDir.exists()){
                dataDir.mkdirs();
            }

            // 如果数据库不存在 则进行建库建表
            createTable();
            // 插入默认数据
            createDefaultData();
            log.info("[DataBaseManager]:Database initialization is complete");
        }else{
            log.info("[DataBaseManager]:Database is already exists");
        }
    }


    // 判断数据库是否存在（即判断数据库连接时指定的 url: jdbc:sqlite:./data/meta.db 这个文件是否存在）
    private boolean checkDBExists() {
        File file = new File("./data/meta.db");
        return file.exists();
    }

    // 删除数据库
    public void deleteDB(){
        File file = new File("./data/meta.db");
        boolean ret = file.delete();
        if(ret){
            log.info("[DataBaseManager]:Delete Database success");
        }else{
            log.info("[DataBaseManager]:Delete Database fail");
        }

        File dataDir = new File("./data");
        ret = dataDir.delete();
        if(ret){
            log.info("[DataBaseManager]:Delete dataDir success");
        }else{
            log.info("[DataBaseManager]:Delete dataDir fail");
        }
    }

    // ./data/meta.db 不需要手动创建 首次执行数据库操作时会自动创建这个文件（Mybatis自动完成）
    private void createTable() {
        metaMapper.createExchangeTable();
        metaMapper.createQueueTable();
        metaMapper.createBindingTable();
        log.info("[DataBaseManager]: Create Table complete");
    }

    // 给数据库表中添加默认数据（即添加一个默认的交换机（匿名 类型：DIRECT））
    private void createDefaultData() {
        Exchange exchange = new Exchange();
        exchange.setName(""); // 匿名
        exchange.setType(ExchangeType.DIRECT);
        exchange.setDurable(true); // 是否持久化存储
        exchange.setAutoDelete(false); // 不自动删除
        metaMapper.insertExchange(exchange);
        log.info("[DataBaseManager]: Create initExchange complete");
    }

    // 其他的数据库操作也封装到这个类中
    public void insertExchange(Exchange exchange) {
        metaMapper.insertExchange(exchange);
    }

    public List<Exchange> getExchangeList() {
        return metaMapper.getExchangeList();
    }

    public void deleteExchange(String exchangeName) {
        metaMapper.deleteExchange(exchangeName);
    }

    public void insertQueue(MSGQueue queue) {
        metaMapper.insertQueue(queue);
    }

    public List<MSGQueue> getQueueList() {
        return metaMapper.getQueueList();
    }

    public void deleteQueue(String queueName) {
        metaMapper.deleteQueue(queueName);
    }

    public void insertBinding(Binding binding) {
        metaMapper.insertBinding(binding);
    }

    public List<Binding> getBindingList() {
        return metaMapper.getBindingList();
    }

    public void deleteBinding(Binding binding) {
        metaMapper.deleteBinding(binding);
    }

}
