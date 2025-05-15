package com.fly.mq;

import com.fly.mq.mqserver.core.Binding;
import com.fly.mq.mqserver.core.Exchange;
import com.fly.mq.mqserver.core.ExchangeType;
import com.fly.mq.mqserver.core.MSGQueue;
import com.fly.mq.mqserver.dao.DataBaseManager;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

@Slf4j
@SpringBootTest
public class DataBaseManagerTests {
    DataBaseManager dataBaseManager = new DataBaseManager();
    // 方法执行前的准备工作
   @BeforeEach
    public void setUp(){
        MqApplication.context = SpringApplication.run(MqApplication.class);
        dataBaseManager.init();
    }

    // 收尾工作
    @AfterEach
    public void tearDown(){
        MqApplication.context.close();
        dataBaseManager.deleteDB();
    }

    // 测试初始化
    @Test
    public void testInit(){
        // 查询结果预期应该是 Exchange(匿名 DIRECT) -> 1 queue -> 0 binding -> 0
        List<Exchange> exchangeList = dataBaseManager.getExchangeList();
        List<MSGQueue> queueList = dataBaseManager.getQueueList();
        List<Binding> bindingList = dataBaseManager.getBindingList();

        Assertions.assertEquals(1, exchangeList.size());
        Assertions.assertEquals("", exchangeList.get(0).getName());
        Assertions.assertEquals(ExchangeType.DIRECT, exchangeList.get(0).getType());
        Assertions.assertEquals(0, queueList.size());
        Assertions.assertEquals(0, bindingList.size());

    }

    // 创建一个交换机进行测试
    private Exchange createTestExchange(String exchangeName){
       Exchange exchange = new Exchange();
       exchange.setName(exchangeName);
       exchange.setType(ExchangeType.FANOUT);
       exchange.setAutoDelete(false);
       exchange.setDurable(true);
       exchange.setArguments("test1","1");
       exchange.setArguments("test2","2");
       return exchange;
    }

    // 测试插入/新增交换机
    @Test
    public void testExchange(){
        Exchange exchange = createTestExchange("testExchange");
        dataBaseManager.insertExchange(exchange);

        List<Exchange> exchangeList = dataBaseManager.getExchangeList();
        Exchange exchange1 = exchangeList.get(1);
        Assertions.assertEquals(2, exchangeList.size());
        Assertions.assertEquals("testExchange", exchange1.getName());
        Assertions.assertEquals(ExchangeType.FANOUT, exchange1.getType());
        Assertions.assertFalse(exchange1.isAutoDelete());
        Assertions.assertTrue(exchange1.isDurable());
        Assertions.assertEquals("1", exchange1.getArguments("test1"));
        Assertions.assertEquals("2", exchange1.getArguments("test2"));

    }

    // 测试删除交换机
    @Test
    public void testDeleteExchange(){
       Exchange exchange = new Exchange();
       exchange.setName("testDeleteExchange");
       dataBaseManager.insertExchange(exchange);
       List<Exchange> exchangeList = dataBaseManager.getExchangeList();
       Assertions.assertEquals(2, exchangeList.size());
       Assertions.assertEquals("testDeleteExchange", exchangeList.get(1).getName());

       dataBaseManager.deleteExchange(exchangeList.get(1).getName());
       exchangeList = dataBaseManager.getExchangeList();
       Assertions.assertEquals(1, exchangeList.size());
       Assertions.assertEquals("", exchangeList.get(0).getName());
    }

    private MSGQueue createTestQueue(String queueName){
       MSGQueue queue = new MSGQueue();
       queue.setName(queueName);
       queue.setDurable(true);
       queue.setAutoDelete(false);
       queue.setArguments("test1",1);
       queue.setArguments("test2",2);
       return queue;
    }

    // 测试新增/创建 queue
    @Test
    public void testCreateQueue(){
        MSGQueue queue = createTestQueue("testCreateQueue");
        dataBaseManager.insertQueue(queue);
        List<MSGQueue> queueList = dataBaseManager.getQueueList();
        Assertions.assertEquals(1, queueList.size());
        Assertions.assertEquals("testCreateQueue", queueList.get(0).getName());
        Assertions.assertTrue(queueList.get(0).isDurable());
        Assertions.assertFalse(queueList.get(0).isAutoDelete());
        Assertions.assertEquals(1, queueList.get(0).getArguments("test1"));
        Assertions.assertEquals(2, queueList.get(0).getArguments("test2"));

    }

    // 测试删除 queue
    @Test
    public void testDeleteQueue(){
       MSGQueue queue = new MSGQueue();
       queue.setName("testDeleteQueue");
       dataBaseManager.insertQueue(queue);
       List<MSGQueue> queueList = dataBaseManager.getQueueList();
       Assertions.assertEquals(1, queueList.size());
       Assertions.assertEquals("testDeleteQueue", queueList.get(0).getName());
       dataBaseManager.deleteQueue(queueList.get(0).getName());
       queueList = dataBaseManager.getQueueList();
       Assertions.assertEquals(0, queueList.size());
    }

    private Binding createTestBinding(String exchangeName, String queueName){
       log.info("exchangeName:{}, queueName:{}", exchangeName, queueName);
       Binding binding = new Binding();
       binding.setExchangeName(exchangeName);
       binding.setQueueName(queueName);
       binding.setBindingKey("testBindingKey");
       return binding;
    }

    // 测试绑定
    @Test
    public void testCreateBinding(){
       Binding binding = createTestBinding("testBindingExchange","testBindingQueue");
       dataBaseManager.insertBinding(binding);
       List<Binding> bindingList = dataBaseManager.getBindingList();
       Binding binding1 = bindingList.get(0);
        System.out.println(binding1);
       Assertions.assertEquals(1, bindingList.size());
       Assertions.assertEquals("testBindingExchange", bindingList.get(0).getExchangeName());
       Assertions.assertEquals("testBindingQueue", bindingList.get(0).getQueueName());
       Assertions.assertEquals("testBindingKey", binding.getBindingKey());

    }


    // 删除绑定
    @Test
    public void testDeleteBinding(){
       Binding binding = new Binding();
       binding.setExchangeName("testDeleteBindingExchange");
       binding.setQueueName("testDeleteBindingQueue");
       binding.setBindingKey("testDeleteBindingKey");
       dataBaseManager.insertBinding(binding);
       List<Binding> bindingList = dataBaseManager.getBindingList();
       Assertions.assertEquals(1, bindingList.size());
       Assertions.assertEquals("testDeleteBindingExchange", bindingList.get(0).getExchangeName());
       Assertions.assertEquals("testDeleteBindingQueue", bindingList.get(0).getQueueName());
       Assertions.assertEquals("testDeleteBindingKey", binding.getBindingKey());
       dataBaseManager.deleteBinding(binding);
       bindingList = dataBaseManager.getBindingList();
       Assertions.assertEquals(0, bindingList.size());
    }
}
