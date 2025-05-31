package com.fly.mq.common;

import com.fly.mq.mqserver.core.ExchangeType;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Map;

@Getter
@Setter
public class ExchangeDeclareArguments extends BasicArguments implements Serializable {
    private String exchangeName;
    private ExchangeType exchangeType;
    private boolean durable;
    private boolean autoDelete;
    private Map<String,Object> arguments;

}
