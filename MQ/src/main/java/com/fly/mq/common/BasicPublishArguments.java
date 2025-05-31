package com.fly.mq.common;

import com.fly.mq.mqserver.core.BasicProperties;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class BasicPublishArguments extends BasicArguments implements Serializable {
    private String exchangeName;
    private String routingKey;
    private BasicProperties basicProperties;
    private byte[] body;
}
