package com.fly.mq;


import com.fly.mq.common.Consumer;
import com.fly.mq.mqclient.Channel;
import com.fly.mq.mqclient.Connection;
import com.fly.mq.mqclient.ConnectionFactory;
import com.fly.mq.mqserver.BrokerServer;
import com.fly.mq.mqserver.core.BasicProperties;
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

@SpringBootTest
public class MQClientTests {
    private BrokerServer brokerServer;
    private ConnectionFactory connectionFactory;
    private Thread thread;

    @BeforeEach
    public void setUp() throws IOException {
        // 1.启动服务器
        MqApplication.context = SpringApplication.run(MqApplication.class);
        brokerServer = new BrokerServer(9090);
        // 由于 start 之后会进入死循环所以这里创建一个新的线程去执行
        thread = new Thread(() -> {
            try {
                brokerServer.start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        thread.start();

        // 2.配置 ConnectionFactory
        connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("127.0.0.1");
        connectionFactory.setPort(9090);
    }

    @AfterEach
    public void tearDown() throws IOException {
        // 停止服务器
        brokerServer.stop();
        //thread.join();

        MqApplication.context.close();

        // 删除目录文件
        File fileDir = new File("./data");
        FileUtils.deleteDirectory(fileDir);
        connectionFactory = null;
    }

    @Test
    public void testConnection() throws IOException {
        Connection connection = connectionFactory.newConnection();
        Assertions.assertNotNull(connection);
    }

    @Test
    public void testChannel() throws IOException {
        Connection connection = connectionFactory.newConnection();
        Assertions.assertNotNull(connection);
        Channel channel = connection.createChannel();
        Assertions.assertNotNull(channel);
    }

    @Test
    public void testExchange() throws IOException {
        Connection connection = connectionFactory.newConnection();
        Assertions.assertNotNull(connection);
        Channel channel = connection.createChannel();
        Assertions.assertNotNull(channel);
        boolean ok = channel.exchangeDeclare("testExchange", ExchangeType.DIRECT,true,false,null);
        Assertions.assertTrue(ok);

        ok = channel.exchangeDelete("testExchange");
        Assertions.assertTrue(ok);

        channel.close();
        connection.close();
    }

    @Test
    public void testQueue() throws IOException {
        Connection connection = connectionFactory.newConnection();
        Assertions.assertNotNull(connection);
        Channel channel = connection.createChannel();
        Assertions.assertNotNull(channel);
        boolean ok = channel.queueDeclare("testQueue", true, false, false, null);
        Assertions.assertTrue(ok);
        ok = channel.queueDelete("testQueue");
        Assertions.assertTrue(ok);
    }

    @Test
    public void testBinding() throws IOException {
        Connection connection = connectionFactory.newConnection();
        Assertions.assertNotNull(connection);
        Channel channel = connection.createChannel();
        Assertions.assertNotNull(channel);

        boolean ok = channel.exchangeDeclare("testExchange", ExchangeType.DIRECT,true,false,null);
        Assertions.assertTrue(ok);
        ok = channel.queueDeclare("testQueue", true, false, false, null);
        Assertions.assertTrue(ok);

        ok = channel.queueBind("testExchange", "testQueue", "aaa.bbb.ccc");
        Assertions.assertTrue(ok);
        ok = channel.queueUnbind("testExchange", "testQueue");
        Assertions.assertTrue(ok);

        channel.close();
        connection.close();
    }

    @Test
    public void testMessage() throws IOException, InterruptedException {
        Connection connection = connectionFactory.newConnection();
        Assertions.assertNotNull(connection);
        Channel channel = connection.createChannel();
        Assertions.assertNotNull(channel);

        boolean ok = channel.exchangeDeclare("testExchange", ExchangeType.DIRECT,true,false,null);
        Assertions.assertTrue(ok);
        ok = channel.queueDeclare("testQueue", true, false, false, null);
        Assertions.assertTrue(ok);

        byte[] requestBody = "Hello World".getBytes();
        ok = channel.basicPublish("testExchange", "testQueue", null, requestBody);
        Assertions.assertTrue(ok);

        ok = channel.basicConsume("testQueue", false, new Consumer() {
            @Override
            public void handleDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) throws IOException {
                System.out.println("Consume begin");
                System.out.println("Consumer tag: " + consumerTag);
                System.out.println("basicProperties: " + basicProperties);
                Assertions.assertArrayEquals(requestBody, body);
                System.out.println("Consume end");
                boolean ok = channel.basicAck("testQueue",basicProperties.getMessageId());
                Assertions.assertTrue(ok);
            }
        });
        Assertions.assertTrue(ok);
        Thread.sleep(500);

        channel.close();
        connection.close();
    }


}
