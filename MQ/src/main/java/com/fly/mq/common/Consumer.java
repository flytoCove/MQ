package com.fly.mq.common;

import com.fly.mq.mqserver.core.BasicProperties;
import com.fly.mq.mqserver.core.Message;

import java.io.IOException;

/**
 * 每次服务器收到消息之后调用
 * 将消息推送给消费者
 */
@FunctionalInterface
public interface Consumer {
    // Delivery 投递
    void handleDelivery(String consumerTag, BasicProperties basicProperties,byte[] body) throws IOException;
}
