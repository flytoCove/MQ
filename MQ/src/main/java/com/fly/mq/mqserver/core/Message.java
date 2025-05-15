package com.fly.mq.mqserver.core;

import lombok.Data;

import java.io.Serializable;
import java.util.UUID;

/**
 * 表示一个要传递的消息
 * 需要写入文件和网络传输（需要序列化和反序列化）实现（Serializable）
 */

@Data
public class Message implements Serializable {
    // 消息的基本属性
    private BasicProperties basicProperties = new BasicProperties();
    private byte[] body;

    // 用于消息持久化到文件中时查找的偏移量 [offsetBeg, offsetEnd)
    // 文件起始位置距离消息起始位置的偏移量（字节）
    // 不需要序列化 transient
    private transient long offsetBeg;
    // 文件起始位置距离消息结束位置的偏移量（字节）
    private transient long offsetEnd;

    // 表示这条消息在文件中是否有效（逻辑删除） 0x1 有效 0x0 无效
    private byte isValid = 0x1;

    // 创建一个工厂方法 封装一下 Message 的创建过程
    // 创建的 Message 会生成一个唯一的 messageId
    // 如果传入的 routingKey 和 BasicProperties 里的冲突以传入的为主
    public static Message createMessageById(String routingKey,BasicProperties basicProperties,byte[] body) {
        Message message = new Message();
        if(basicProperties != null) {
            message.setBasicProperties(basicProperties);
        }

        // 此时生成的 messageId 以 M- 作为前缀
        message.setMessageId("M-" + UUID.randomUUID());
        message.setRoutingKey(routingKey);
        message.setBody(body);

        return message;
    }

    // 为了后续使用方便
    public String getMessageId() {
        return basicProperties.getMessageId();
    }

    public void setMessageId(String messageId) {
        basicProperties.setMessageId(messageId);
    }

    public String getRoutingKey() {
        return basicProperties.getRoutingKey();
    }

    public void setRoutingKey(String routingKey) {
        basicProperties.setRoutingKey(routingKey);
    }

    public int getDeliverMode() {
        return basicProperties.getDeliverMode();
    }

    public void setDeliverMode(int deliverMode) {
        basicProperties.setDeliverMode(deliverMode);
    }
}
