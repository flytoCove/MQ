package com.fly.mq.common;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class QueueDeleteArguments extends BaseArguments implements Serializable {
    private String queueName;
}
