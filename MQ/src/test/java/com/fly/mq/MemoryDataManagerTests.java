package com.fly.mq;

import com.fly.mq.mqserver.core.*;
import com.fly.mq.mqserver.dao.DiskDataCenter;
import com.fly.mq.mqserver.dao.MemoryDataManager;
import org.apache.tomcat.util.http.fileupload.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@SpringBootTest
public class MemoryDataManagerTests {
    private MemoryDataManager memoryDataManager = null;

    @BeforeEach
    public void setUp(){
        memoryDataManager = new MemoryDataManager();
    }
    @AfterEach
    public void tearDown(){
        memoryDataManager = null;
    }

    // 创建测试交换机
    private Exchange createExchangeTest(String exchangeName){
        Exchange exchange = new Exchange();
        exchange.setName(exchangeName);
        exchange.setType(ExchangeType.DIRECT);
        exchange.setDurable(true);
        exchange.setAutoDelete(false);
        return exchange;
    }

    // 创建测试队列
    private MSGQueue createQueueTest(String queueName){
        MSGQueue queue = new MSGQueue();
        queue.setName(queueName);
        queue.setDurable(true);
        queue.setAutoDelete(false);
        queue.setExclusive(false);
        return queue;
    }

    // 针对交换机查询
    @Test
    public void testExchange(){
        // 1.创建一个交换机
        Exchange exceptedExchange = createExchangeTest("testExchange");
        memoryDataManager.insertExchange(exceptedExchange);

        // 2.查询交换此是否存在 比较结果是否一致
        Exchange actualExchange = memoryDataManager.getExchange(exceptedExchange.getName());
        Assertions.assertEquals(exceptedExchange, actualExchange);

        // 3.删除交换机
        memoryDataManager.deleteExchange(exceptedExchange.getName());

        // 4.在查询一次看是否存在
        actualExchange = memoryDataManager.getExchange(exceptedExchange.getName());
        Assertions.assertNull(actualExchange);

    }

    // 针对队列测试
    @Test
    public void testQueue(){
        MSGQueue exceptedQueue = createQueueTest("testQueue");
        memoryDataManager.insertQueue(exceptedQueue);

        MSGQueue actualQueue = memoryDataManager.getQueue("testQueue");
        Assertions.assertEquals(exceptedQueue, actualQueue);

        memoryDataManager.deleteQueue("testQueue");

        actualQueue = memoryDataManager.getQueue("testQueue");
        Assertions.assertNull(actualQueue);
    }

    // 针对绑定测试
    @Test
    public void testBinding(){
        Binding exceptedBinding = new Binding();
        exceptedBinding.setExchangeName("testExchange");
        exceptedBinding.setQueueName("testQueue");
        exceptedBinding.setBindingKey("testBindingKey");
        memoryDataManager.insertBinding(exceptedBinding);
        Binding actualBinding = memoryDataManager.getBinding("testExchange", "testQueue");
        Assertions.assertEquals(exceptedBinding, actualBinding);

        ConcurrentHashMap<String,Binding> bingMap = memoryDataManager.getBindings("testExchange");
        Assertions.assertEquals(exceptedBinding, bingMap.get("testQueue"));
        Assertions.assertEquals(1, bingMap.size());

        memoryDataManager.deleteBinding(exceptedBinding);
        actualBinding = memoryDataManager.getBinding("testExchange", "testQueue");
        Assertions.assertNull(actualBinding);
    }

    private Message createMessageTest(String content){
        return Message.createMessageById("testRoutingKey",null,content.getBytes());
    }

    // 针对消息测试
    @Test
    public void testMessage(){
        Message exceptedMessage = createMessageTest("testMessage");
        memoryDataManager.addMessage(exceptedMessage);

        Message actualMessage = memoryDataManager.getMessage(exceptedMessage.getMessageId());
        Assertions.assertEquals(exceptedMessage, actualMessage);

        memoryDataManager.removeMessage(exceptedMessage.getMessageId());

        actualMessage = memoryDataManager.getMessage(exceptedMessage.getMessageId());
        Assertions.assertNull(actualMessage);
    }

    @Test
    public void testSendMessage(){
        // 1.创建一个队列 创建 10 条消息 将这些消息插入队列中
        MSGQueue queue = createQueueTest("testMessageQueue");
        List<Message> exceptedMessages = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Message message = createMessageTest("testMessage" + i);
            memoryDataManager.sendMessage(queue, message);
            exceptedMessages.add(message);
        }

        // 2.从队列中取出这些消息
        List<Message> actualMessage = new ArrayList<>();
        while(true){
            Message message = memoryDataManager.pollMessage(queue.getName());
            if(message == null){
                break;
            }
            actualMessage.add(message);
        }
        // 3.取出消息和之前的进行对比
        Assertions.assertEquals(exceptedMessages.size(), actualMessage.size());
        for(int i = 0; i < exceptedMessages.size(); i++){
            Assertions.assertEquals(exceptedMessages.get(i), actualMessage.get(i));
        }
    }

    @Test
    public void testMessageWaitAck(){
        Message exceptedMessage = createMessageTest("testMessageWaitAck");
        memoryDataManager.addMessageWaitAck("testQueue", exceptedMessage);
        Message actualMessage = memoryDataManager.pollMessageWaitAck("testQueue", exceptedMessage.getMessageId());
        Assertions.assertEquals(exceptedMessage, actualMessage);
        memoryDataManager.removeMessageWaitAck("testQueue", exceptedMessage.getMessageId());
        actualMessage = memoryDataManager.getMessage(exceptedMessage.getMessageId());
        Assertions.assertNull(actualMessage);
    }

    @Test
    public void testRecovery() throws IOException, ClassNotFoundException {
        // 后续数据库操作依赖 MyBatis 需要启动 SpringApplication
        MqApplication.context = SpringApplication.run(MqApplication.class);
        // 1.在硬盘上构造数据
        DiskDataCenter diskDataCenter = new DiskDataCenter();
        diskDataCenter.init();

        // 构造交换机
        Exchange exceptedExchange = createExchangeTest("testExchange");
        diskDataCenter.insertExchange(exceptedExchange);

        // 构造队列
        MSGQueue exceptedQueue = createQueueTest("testQueue");
        diskDataCenter.insertQueue(exceptedQueue);

        // 构造绑定
        Binding exceptedBinding = new Binding();
        exceptedBinding.setExchangeName("testExchange");
        exceptedBinding.setQueueName("testQueue");
        exceptedBinding.setBindingKey("testBindingKey");
        diskDataCenter.insertBinding(exceptedBinding);

        // 构造消息
        Message exceptedMessage = createMessageTest("testMessage");
        diskDataCenter.sendMessage(exceptedQueue,exceptedMessage);

        // 2.执行恢复操作
        memoryDataManager.recovery(diskDataCenter);

        // 3.对比结果
        Exchange actualExchange = memoryDataManager.getExchange("testExchange");
        Assertions.assertEquals(exceptedExchange.getName(), actualExchange.getName());
        Assertions.assertEquals(exceptedExchange.getType(), actualExchange.getType());
        Assertions.assertEquals(exceptedExchange.isAutoDelete(), actualExchange.isAutoDelete());
        Assertions.assertEquals(exceptedExchange.isDurable(), actualExchange.isDurable());

        MSGQueue actualQueue = memoryDataManager.getQueue("testQueue");
        Assertions.assertEquals(exceptedQueue.getName(), actualQueue.getName());
        Assertions.assertEquals(exceptedQueue.isExclusive(), actualQueue.isExclusive());
        Assertions.assertEquals(exceptedQueue.isDurable(), actualQueue.isDurable());
        Assertions.assertEquals(exceptedQueue.isAutoDelete(), actualQueue.isAutoDelete());

        Binding actualBinding = memoryDataManager.getBinding("testExchange", "testQueue");
        Assertions.assertEquals(exceptedBinding.getExchangeName(), actualBinding.getExchangeName());
        Assertions.assertEquals(exceptedBinding.getQueueName(), actualBinding.getQueueName());
        Assertions.assertEquals(exceptedBinding.getBindingKey(), actualBinding.getBindingKey());


        Message actualMessage = memoryDataManager.pollMessage(exceptedQueue.getName());
        Assertions.assertEquals(exceptedMessage.getMessageId(), actualMessage.getMessageId());
        Assertions.assertEquals(exceptedMessage.getDeliverMode(), actualMessage.getDeliverMode());
        Assertions.assertEquals(exceptedMessage.getRoutingKey(), actualMessage.getRoutingKey());
        Assertions.assertArrayEquals(exceptedMessage.getBody(), actualMessage.getBody());

        // 清理硬盘数据
        MqApplication.context.close();
        File dataDir = new File("./data");
        FileUtils.deleteDirectory(dataDir);

    }

}
