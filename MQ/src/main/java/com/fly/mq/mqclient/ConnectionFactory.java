package com.fly.mq.mqclient;

import lombok.Getter;
import lombok.Setter;

import java.io.IOException;

@Getter
@Setter
public class ConnectionFactory {
    // BrokerServer 的地址和端口号
    private String host;
    private int port;

    // 扩展部分 TODO

    public Connection newConnection() throws IOException {
        Connection connection = new Connection(host,port);
        return connection;
    }
}
