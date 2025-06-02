package com.fly.mq.demo;

import com.fly.mq.common.Consumer;
import com.fly.mq.common.MQException;
import com.fly.mq.mqclient.Channel;
import com.fly.mq.mqclient.Connection;
import com.fly.mq.mqclient.ConnectionFactory;
import com.fly.mq.mqserver.core.BasicProperties;
import com.fly.mq.mqserver.core.ExchangeType;

import java.io.IOException;

/**
 * 表示一个消费者.
 * 通常这个类也应该是在一个独立的服务器中被执行
 */
public class DemoConsumer {
    public static void main(String[] args) throws IOException, MQException, InterruptedException {
        System.out.println("启动消费者!");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");
        factory.setPort(9090);

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare("testExchange", ExchangeType.DIRECT, true, false, null);
        channel.queueDeclare("testQueue", true, false, false, null);

        channel.basicConsume("testQueue", true, new Consumer() {
            @Override
            public void handleDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) throws MQException {
                System.out.println("[消费数据] 开始!");
                System.out.println("consumerTag=" + consumerTag);
                System.out.println("basicProperties=" + basicProperties);
                String bodyString = new String(body);
                System.out.println("body=" + bodyString);
                System.out.println("[消费数据] 结束!");
            }
        });

        // 由于消费者也不知道生产者要生产多少, 就在这里通过这个循环模拟一直等待消费.
        while (true) {
            Thread.sleep(500);
        }
    }
}
