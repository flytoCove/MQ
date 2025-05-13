package com.fly.mq.mqserver.core;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/**
 * 表示交换机
 */
@Data
public class Exchange {
    // 表示交换机的唯一标识
    private String name;
    //表示交换机类型
    private ExchangeType type = ExchangeType.DIRECT;
    //表示该交换机是否需要持久化存储
    private boolean durable = false;
    //表示是否自动删除
    private boolean autoDelete = false;
    //表示创建交换机是指定的一些额外的选项
    private Map<String, Object> arguments = new HashMap<>();
}
