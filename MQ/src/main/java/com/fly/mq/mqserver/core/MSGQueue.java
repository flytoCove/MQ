package com.fly.mq.mqserver.core;

//为了避免和库里的 Queue 名称混淆 这里取名 MSGQueue => message queue

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/**
 * 表示存储消的队列
 */
@Data
public class MSGQueue {
    // 表示队列的唯一标识
    private String name;
    //表示该队列是否需要持久化存储
    private boolean durable = false;
    //没人使用是否自动删除
    private boolean autoDelete = false;
    //为 true 表示这个队列只能被一个消费者使用
    private boolean exclusive = false;
    //表示创建交换机是指定的一些额外的选项
    private Map<String, Object> arguments = new HashMap<>();
}
