package com.fly.mq.mqserver;

import com.fly.mq.common.*;
import com.fly.mq.mqserver.core.BasicProperties;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 消息队列本体服务器
 * 本质就是一个 TCP 服务器
 */
public class BrokerServer {
    private ServerSocket serverSocket;

    // 默认一个 BrokerServer 上一个虚拟主机
    private VirtualHost virtualHost = new VirtualHost("default");

    // 表示当前所有会话（有哪些客户端正在和服务器通信）
    // key: channelId value: 对应的 Socket 对象
    private ConcurrentHashMap<String, Socket> sessions = new ConcurrentHashMap<>();

    // 线程池 用于处理多个客户端请求
    private ExecutorService executorService = null;

    // 用来控制服务器是否继续运行
    private volatile boolean runnable = true;

    public BrokerServer(int port) throws IOException {
        serverSocket = new ServerSocket(port);
    }

    public void start() throws IOException {
        System.out.println("[Starting BrokerServer]");
        executorService = Executors.newCachedThreadPool();
        while (runnable) {
            Socket clientAccept = serverSocket.accept();
            // 处理连接的逻辑丢给线程池
            executorService.submit(() -> {
                processConnection(clientAccept);
            });
        }
    }

    // 用于单元测试
    public void stop() throws IOException {
        runnable = false;
        // 停止线程池
        executorService.shutdownNow();
        // 关闭连接
        serverSocket.close();
    }

    // 处理一个客户端连接
    // 一个连接涉及多个请求和响应
    private void processConnection(Socket clientAccept) {
        try (InputStream inputStream = clientAccept.getInputStream();
             OutputStream outputStream = clientAccept.getOutputStream()) {
            try (DataInputStream dataInputStream = new DataInputStream(clientAccept.getInputStream());
                 DataOutputStream dataOutputStream = new DataOutputStream(outputStream)) {
                while (true) {
                    // 1.读取请求并解析
                    Request request = readRequest(dataInputStream);
                    // 2.根据请求计算响应
                    Response response = process(request, clientAccept);
                    // 3.把响应写回给客户端
                    writeResponse(dataOutputStream, response);
                }
            } catch (EOFException | SocketException e) {
                //DataInputStream 读到 EOF(文件结尾) 会抛出 EOFException 异常
                // 这里借助这个异常退出循环
                System.out.println("[BrokerServer] connection closed by client IP: " + clientAccept.getInetAddress() + "Port: " + clientAccept.getPort());
            }

        } catch (Exception e) {
            System.out.println("[BrokerServer] connection Error");
            e.printStackTrace();
        } finally {
            try {
                clientAccept.close();
                // 一个 TCP 连接中可能有多个 Channel 把 socket 中的所有 Channel 都顺便清理掉
                clearClosedSession(clientAccept);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    private Request readRequest(DataInputStream dataInputStream) throws IOException {
        try {
            Request request = new Request();
            request.setType(dataInputStream.readInt());
            request.setLength(dataInputStream.readInt());
            byte[] payload = new byte[request.getLength()];
            dataInputStream.readFully(payload);
            request.setPayload(payload);
            return request;
        } catch (Exception e) {
            throw new IOException(e.getMessage());
        }
    }

    private void writeResponse(DataOutputStream dataOutputStream, Response response) throws IOException {
        dataOutputStream.writeInt(response.getType());
        dataOutputStream.writeInt(response.getLength());
        dataOutputStream.write(response.getPayload());
        dataOutputStream.flush();
    }

    private Response process(Request request, Socket clientAccept) throws IOException, ClassNotFoundException {
        // 把 Request 中的数据做初步解析
        BasicArguments basicArguments = (BasicArguments) BinaryTool.fromBytes(request.getPayload());
        System.out.println("[Request] rid: " + basicArguments.getRid() + " channelId: " + basicArguments.getChannelId() + " type: "
                + request.getType() + "length: " + request.getLength());

        // 根据 type 进行具体要做什么
        boolean ok = true;
        if (request.getType() == 0x1) {
            sessions.put(basicArguments.getChannelId(), clientAccept);
            System.out.println("[BrokerServer] create channel success channelId: " + basicArguments.getChannelId());
        }
        else if (request.getType() == 0x2) {
            sessions.remove(basicArguments.getChannelId());
            System.out.println("[BrokerServer] remove channel success channelId: " + basicArguments.getChannelId());
        }
        else if (request.getType() == 0x3) {
            // 0x3 创建交换机 说明 payload 是一个 ExchangeDeclareArguments 对象
            ExchangeDeclareArguments arguments = (ExchangeDeclareArguments) basicArguments;
            ok = virtualHost.exchangeDeclare(arguments.getExchangeName(), arguments.getExchangeType(),
                    arguments.isDurable(), arguments.isAutoDelete(), arguments.getArguments());
        }
        else if (request.getType() == 0x4) {
            ExchangeDeleteArguments arguments = (ExchangeDeleteArguments) basicArguments;
            ok = virtualHost.exchangeDelete(arguments.getExchangeName());
        }
        else if (request.getType() == 0x5) {
            QueueDeclareArguments arguments = (QueueDeclareArguments) basicArguments;
            ok = virtualHost.queueDeclare(arguments.getQueueName(), arguments.isDurable(), arguments.isExclusive(),
                    arguments.isAutoDelete(), arguments.getArguments());
        }
        else if (request.getType() == 0x6) {
            QueueDeleteArguments arguments = (QueueDeleteArguments) basicArguments;
            ok = virtualHost.queueDelete(arguments.getQueueName());
        }
        else if (request.getType() == 0x7) {
            QueueBindArguments arguments = (QueueBindArguments) basicArguments;
            ok = virtualHost.queueBind(arguments.getExchangeName(), arguments.getQueueName(), arguments.getBindingKey());
        }
        else if (request.getType() == 0x8) {
            QueueUnBindArguments arguments = (QueueUnBindArguments) basicArguments;
            ok = virtualHost.queueUnbind(arguments.getExchangeName(), arguments.getQueueName());
        }
        else if (request.getType() == 0x9) {
            BasicPublishArguments arguments = (BasicPublishArguments) basicArguments;
            ok = virtualHost.basicPublish(arguments.getExchangeName(), arguments.getRoutingKey(), arguments.getBasicProperties(), arguments.getBody());
        }
        else if (request.getType() == 0xa) {
            BasicConsumeArguments arguments = (BasicConsumeArguments) basicArguments;
            virtualHost.basicConsume(arguments.getConsumerTag(), arguments.getQueueName(), arguments.isAutoAck(), new Consumer() {
                @Override
                public void handleDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) throws IOException {
                    // 回调的工作就是把服务器收到的消息推送给对应的消费者客户端
                    // 此处的 consumerTag 就是 channelId 使用这个 channelId 去 sessions 中查找对应的 socket 对象
                    // 然后往里面发消息

                    // 1.找到 socket 对象
                    Socket clientSocket = sessions.get(consumerTag);
                    if(clientSocket == null || clientSocket.isConnected()) {
                        System.out.println("[BrokerServer] connection closed");
                    }

                    // 2.构造响应数据
                    SubScribeReturns subScribeReturns = new SubScribeReturns();
                    subScribeReturns.setChannelId(consumerTag);
                    // 此处的 rid 没有对应的请求不设置也可以 此处暂时设置成 ""
                    subScribeReturns.setRid("");
                    subScribeReturns.setOk(true);
                    subScribeReturns.setConsumerTag(consumerTag);
                    subScribeReturns.setProperties(basicProperties);
                    subScribeReturns.setBody(body);
                    byte[] payload = BinaryTool.toBytes(subScribeReturns);


                    Response response = new Response();
                    // 0xc 表示服务器给消费者客户端推送消息数据
                    response.setType(0xc);
                    response.setLength(payload.length);
                    // response 的 payload 则是一个 SubScribeReturns
                    response.setPayload(payload);

                    // 把数据写回给客户端
                    // 这里不能直接 close dataOutputStream 否则也会把 clientSocket 也关闭了 就无法继续往 socket 里写入数据了
                    DataOutputStream dataOutputStream = new DataOutputStream(clientSocket.getOutputStream());
                    writeResponse(dataOutputStream, response);
                }
            });

        }

        return null;
    }


    private void clearClosedSession(Socket clientAccept) {
    }

}
