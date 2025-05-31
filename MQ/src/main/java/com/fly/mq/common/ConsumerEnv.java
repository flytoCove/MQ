package com.fly.mq.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 表示一个消费者（完整的执行环境）
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ConsumerEnv {
    private String consumerTag;
    private String queueName;
    private boolean autoAck;
    // 通过这个函数式接口处理收到的消息
    private Consumer consumer;

}
