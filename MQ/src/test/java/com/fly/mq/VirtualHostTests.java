package com.fly.mq;

import com.fly.mq.common.Consumer;
import com.fly.mq.mqserver.VirtualHost;
import com.fly.mq.mqserver.core.BasicProperties;
import com.fly.mq.mqserver.core.Exchange;
import com.fly.mq.mqserver.core.ExchangeType;
import org.apache.tomcat.util.http.fileupload.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.File;
import java.io.IOException;
import java.sql.SQLOutput;

@SpringBootTest
public class VirtualHostTests {
    VirtualHost virtualHost = null;
    @BeforeEach
    public void setUp(){
        MqApplication.context = SpringApplication.run(MqApplication.class);
        virtualHost = new VirtualHost("default");

    }
    @AfterEach
    public void tearDown() throws IOException {
        MqApplication.context.close();
        virtualHost = null;
        // 删除硬盘数据
        File fileDir = new File("./data");
        FileUtils.deleteDirectory(fileDir);
    }

    @Test
    public void testExchangeDeclare(){
        boolean ok = virtualHost.exchangeDeclare("testExchange", ExchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(ok);
    }

    @Test
    public void testExchangeDelete(){
        boolean ok = virtualHost.exchangeDeclare("testExchange", ExchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(ok);

        ok = virtualHost.exchangeDelete("testExchange");
        Assertions.assertTrue(ok);
    }

    @Test
    public void testQueueDeclare(){
        boolean ok = virtualHost.queueDeclare("testQueue", true, false, false, null);
        Assertions.assertTrue(ok);
    }

    @Test
    public void testQueueDelete(){
        boolean ok = virtualHost.queueDeclare("testQueue", true, false, false, null);
        Assertions.assertTrue(ok);

        ok = virtualHost.queueDelete("testQueue");
        Assertions.assertTrue(ok);
    }

    @Test
    public void testQueueBind(){
        boolean ok = virtualHost.queueDeclare("testQueue", true, false, false, null);
        Assertions.assertTrue(ok);

        ok = virtualHost.exchangeDeclare("testExchange", ExchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(ok);

        ok = virtualHost.queueBind("testExchange", "testQueue", "testBindingKey");
        Assertions.assertTrue(ok);
    }

    @Test
    public void testQueueUnBind(){
        boolean ok = virtualHost.queueDeclare("testQueue", true, false, false, null);
        Assertions.assertTrue(ok);

        ok = virtualHost.exchangeDeclare("testExchange", ExchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(ok);

        ok = virtualHost.queueBind("testExchange", "testQueue", "testBindingKey");
        Assertions.assertTrue(ok);

        ok = virtualHost.queueUnbind("testExchange", "testQueue");
        Assertions.assertTrue(ok);
    }

    // 测试发布消息
    @Test
    public void testBasicPublish(){
        boolean ok = virtualHost.queueDeclare("testQueue", true, false, false, null);
        Assertions.assertTrue(ok);

        ok = virtualHost.exchangeDeclare("testExchange", ExchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(ok);

        ok = virtualHost.queueBind("testExchange", "testQueue", "testBindingKey");
        Assertions.assertTrue(ok);

        ok = virtualHost.basicPublish("testExchange","testQueue",null,"hello".getBytes());
        Assertions.assertTrue(ok);
    }

    // 先订阅队列, 后发送消息
    @Test
    public void testBasicConsume1() throws InterruptedException {
        boolean ok = virtualHost.queueDeclare("testQueue", true,
                false, false, null);
        Assertions.assertTrue(ok);
        ok = virtualHost.exchangeDeclare("testExchange", ExchangeType.DIRECT,
                true, false, null);
        Assertions.assertTrue(ok);

        // 先订阅队列
        ok = virtualHost.basicConsume("testConsumerTag", "testQueue", true, new Consumer() {
            @Override
            public void handleDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) {
                try {
                    // 消费者自身设定的回调方法.
                    System.out.println("messageId=" + basicProperties.getMessageId());
                    System.out.println("body=" + new String(body));

                    Assertions.assertEquals("testQueue", basicProperties.getRoutingKey());
                    Assertions.assertEquals(1, basicProperties.getDeliverMode());
                    Assertions.assertArrayEquals("hello".getBytes(), body);
                } catch (Error e) {
                    // 断言如果失败, 抛出的是 Error, 而不是 Exception!
                    e.printStackTrace();
                    System.out.println("error");
                }
            }
        });
        Assertions.assertTrue(ok);

        Thread.sleep(500);

        // 再发送消息
        ok = virtualHost.basicPublish("testExchange", "testQueue", null,
                "hello".getBytes());
        Assertions.assertTrue(ok);
    }

    // 先发消息在订阅
    @Test
    public void testBasicConsume2() throws InterruptedException {
        boolean ok = virtualHost.queueDeclare("testQueue", true, false, false, null);
        Assertions.assertTrue(ok);

        ok = virtualHost.exchangeDeclare("testExchange", ExchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(ok);

        // 先发消息
        ok = virtualHost.basicPublish("testExchange","testQueue",null,"hello".getBytes());
        Assertions.assertTrue(ok);


        ok = virtualHost.basicConsume("testConsumerTag", "testQueue", true, new Consumer() {
            @Override
            public void handleDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) {
                // 消费者实现具体的逻辑
                System.out.println("messageId: " + basicProperties.getMessageId());
                System.out.println("message: " + new String(body));

                Assertions.assertEquals("testQueue", basicProperties.getRoutingKey());
                Assertions.assertArrayEquals("hello".getBytes(), body);
                Assertions.assertEquals(1,basicProperties.getDeliverMode());
            }
        });

        Assertions.assertTrue(ok);
        Thread.sleep(500);
    }

    @Test
    public void testBasicConsumeFanout() throws InterruptedException {
        boolean ok = virtualHost.exchangeDeclare("testExchange", ExchangeType.FANOUT, false, false, null);
        Assertions.assertTrue(ok);

        ok = virtualHost.queueDeclare("testQueue1", false, false, false, null);
        Assertions.assertTrue(ok);
        ok = virtualHost.queueBind("testExchange", "testQueue1", "");
        Assertions.assertTrue(ok);

        ok = virtualHost.queueDeclare("testQueue2", false, false, false, null);
        Assertions.assertTrue(ok);
        ok = virtualHost.queueBind("testExchange", "testQueue2", "");
        Assertions.assertTrue(ok);

        // 往交换机中发布一个消息
        ok = virtualHost.basicPublish("testExchange", "", null, "hello".getBytes());
        Assertions.assertTrue(ok);


        // 两个消费者订阅上述的两个队列.
        ok = virtualHost.basicConsume("testConsumer1", "testQueue1", true, new Consumer() {
            @Override
            public void handleDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) {
                System.out.println("consumerTag=" + consumerTag);
                System.out.println("messageId=" + basicProperties.getMessageId());
                System.out.println("message: " + new String(body));
                Assertions.assertArrayEquals("hello".getBytes(), body);
            }
        });
        Assertions.assertTrue(ok);

        ok = virtualHost.basicConsume("testConsumer2", "testQueue2", true, new Consumer() {
            @Override
            public void handleDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) {
                System.out.println("consumerTag=" + consumerTag);
                System.out.println("messageId=" + basicProperties.getMessageId());
                System.out.println("message: " + new String(body));
                Assertions.assertArrayEquals("hello".getBytes(), body);
            }
        });
        Assertions.assertTrue(ok);

        Thread.sleep(500);
    }

    @Test
    public void testBasicConsumeTopic() throws InterruptedException {
        boolean ok = virtualHost.exchangeDeclare("testExchange", ExchangeType.TOPIC, false, false, null);
        Assertions.assertTrue(ok);

        ok = virtualHost.queueDeclare("testQueue", false, false, false, null);
        Assertions.assertTrue(ok);

        ok = virtualHost.queueBind("testExchange", "testQueue", "aaa.*.bbb");
        Assertions.assertTrue(ok);

        ok = virtualHost.basicPublish("testExchange", "aaa.a.bbb", null, "hello".getBytes());
        Assertions.assertTrue(ok);

        ok = virtualHost.basicConsume("testConsumer", "testQueue", true, new Consumer() {
            @Override
            public void handleDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) {
                System.out.println("consumerTag=" + consumerTag);
                System.out.println("messageId=" + basicProperties.getMessageId());
                System.out.println("message: " + new String(body));
                Assertions.assertArrayEquals("hello".getBytes(), body);
            }
        });
        Assertions.assertTrue(ok);

        Thread.sleep(500);
    }

    @Test
    public void basicAck() throws InterruptedException {
        boolean ok = virtualHost.queueDeclare("testQueue", true, false, false, null);
        Assertions.assertTrue(ok);

        ok = virtualHost.exchangeDeclare("testExchange", ExchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(ok);

        // 先发消息
        ok = virtualHost.basicPublish("testExchange","testQueue",null,"hello".getBytes());
        Assertions.assertTrue(ok);


        // [autoAck : false]  这里手动调用 basicAck()
        ok = virtualHost.basicConsume("testConsumerTag", "testQueue", false, new Consumer() {
            @Override
            public void handleDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) {
                // 消费者实现具体的逻辑
                System.out.println("messageId: " + basicProperties.getMessageId());
                System.out.println("message: " + new String(body));

                Assertions.assertEquals("testQueue", basicProperties.getRoutingKey());
                Assertions.assertArrayEquals("hello".getBytes(), body);
                Assertions.assertEquals(1,basicProperties.getDeliverMode());
                boolean ok = virtualHost.basicAck("testQueue", basicProperties.getMessageId());
                Assertions.assertTrue(ok);
            }
        });

        Assertions.assertTrue(ok);
        Thread.sleep(500);
    }

}
