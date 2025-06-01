package com.fly.mq.common;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Map;

@Getter
@Setter
public class QueueDeclareArguments extends BaseArguments implements Serializable {
    private String queueName;
    private boolean durable;
    private boolean exclusive;
    private boolean autoDelete;
    private Map<String,Object> arguments;
}
