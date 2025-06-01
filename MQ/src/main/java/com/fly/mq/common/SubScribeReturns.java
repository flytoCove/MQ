package com.fly.mq.common;

import com.fly.mq.mqserver.core.BasicProperties;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class SubScribeReturns extends BaseReturns implements Serializable {
    private String consumerTag;
    private BasicProperties properties;
    private byte[] body;
}
