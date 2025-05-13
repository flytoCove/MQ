package com.fly.mq.mqserver.core;

import lombok.Getter;

/**
 * 表示交换机的类型的枚举类
 */
@Getter
public enum ExchangeType {
    DIRECT(0),
    FANOUT(1),
    TOPIC(2);

    private final int type;

    ExchangeType(int type) {
        this.type = type;
    }
}
