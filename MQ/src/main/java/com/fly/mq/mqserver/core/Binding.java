package com.fly.mq.mqserver.core;

import lombok.Data;

/**
 * 表示交换机和队列之间的关联关系
 */
@Data
public class Binding {
    private String exchangeName;
    private String queueName;
    // 绑定队列到交换机上时, 指定⼀个字符串为 bindingKey.发送消息指定⼀个字符串为routingKey.
    // 当 routingKey 和 bindingKey 满⾜⼀定的匹配条件的时候, 则把消息投递到指定队列.
    // bindingKey 只在交换机类型为 TOPIC 时才有效. ⽤于和消息中的 routingKey 进⾏匹配.
    private String bindingKey;
}
