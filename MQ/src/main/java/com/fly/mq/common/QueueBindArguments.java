package com.fly.mq.common;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class QueueBindArguments extends BasicArguments implements Serializable {
    private String queueName;
    private String exchangeName;
    private String bindingKey;
}
