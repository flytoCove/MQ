package com.fly.mq.demo;

import com.fly.mq.mqclient.Channel;
import com.fly.mq.mqclient.Connection;
import com.fly.mq.mqclient.ConnectionFactory;
import com.fly.mq.mqserver.core.ExchangeType;

import java.io.IOException;

/**
 * 表示一个生产者.
 * 通常这是一个单独的服务器程序.
 */
public class DemoProducer {
    public static void main(String[] args) throws IOException, InterruptedException {
        System.out.println("启动生产者");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");
        factory.setPort(9090);

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // 创建交换机和队列
        channel.exchangeDeclare("testExchange", ExchangeType.DIRECT, true, false, null);
        channel.queueDeclare("testQueue", true, false, false, null);

        // 创建一个消息并发送
        byte[] body = "hello".getBytes();
        boolean ok = channel.basicPublish("testExchange", "testQueue", null, body);
        System.out.println("消息投递完成! ok=" + ok);

        Thread.sleep(500);
        channel.close();
        connection.close();
    }
}
