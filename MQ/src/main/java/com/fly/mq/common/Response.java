package com.fly.mq.common;

import lombok.Data;

/**
 * 表示一个网络通信中的响应对象 按照自定义应用层协议设计
 */
@Data
public class Response {
    private int type;
    private int length;
    private byte[] payload;
}
