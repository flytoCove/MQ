package com.fly.mq;


import com.fly.mq.mqclient.Channel;
import com.fly.mq.mqclient.Connection;
import com.fly.mq.mqclient.ConnectionFactory;
import com.fly.mq.mqserver.BrokerServer;
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
        //Assertions.assertNotNull(connection);
        Channel channel = connection.createChannel();
        Assertions.assertNotNull(channel);
    }

}
