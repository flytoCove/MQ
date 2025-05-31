package com.fly.mq.common;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * 返回值的公共信息
 */
@Getter
@Setter
public class BasicReturns implements Serializable {
    // 一次请求/响应的身份标识 用于一个请求和响应的匹配
    protected String rid;
    protected String channelId;
    // 表示当前这个远程调用方法的返回值
    protected boolean ok;
}
