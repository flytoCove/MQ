package com.fly.mq.common;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class BasicConsumeArguments extends BaseArguments implements Serializable {
    private String consumerTag;
    private String queueName;
    private boolean autoAck;
}
