package com.fly.mq.common;

import com.fly.mq.mqserver.core.BasicProperties;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class BasicPublishArguments extends BaseArguments implements Serializable {
    private String exchangeName;
    private String routingKey;
    private BasicProperties basicProperties;
    private byte[] body;
}
