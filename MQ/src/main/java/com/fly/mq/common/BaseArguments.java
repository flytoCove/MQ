package com.fly.mq.common;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * 表示方法的公共参数/辅助字段
 * 不同的参数再在具体的子类里表示
 * Serializable 实现序列化
 */
@Getter
@Setter
public class BaseArguments implements Serializable {
    // 一次请求/响应的身份标识 用于一个请求和响应的匹配
    protected String rid;
    protected String channelId;

}
