package com.fly.mq.common;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class QueueUnBindArguments extends BasicArguments implements Serializable {
    private String queueName;
    private String exchangeName;
}
