package com.fly.mq.common;

import com.fly.mq.mqserver.core.BasicProperties;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class BasicConsumeArguments extends BasicArguments implements Serializable {
    private String consumerTag;
    private String queueName;
    private boolean autoAck;
}
