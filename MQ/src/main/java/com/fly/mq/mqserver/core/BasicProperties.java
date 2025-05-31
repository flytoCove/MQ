package com.fly.mq.mqserver.core;

import lombok.Data;

import java.io.Serializable;

@Data
public class BasicProperties implements Serializable {
    // 消息的唯一标识
    private String messageId;
    // 用来和 bindingKey 进行匹配
    private String routingKey;
    // 表示消息是否持久化 1 表示 否 2 表示 是
    private int deliverMode = 1;
}
