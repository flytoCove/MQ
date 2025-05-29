package com.fly.mq.common;

import lombok.Data;

/**
 * 表示一个消费者（完整的执行环境）
 */
@Data
public class ConsumerEnv {
    private String consumerTag;
    private String queueName;
    private boolean autoAck;
    // 通过这个函数式接口处理收到的消息
    private Consumer consumer;

}
