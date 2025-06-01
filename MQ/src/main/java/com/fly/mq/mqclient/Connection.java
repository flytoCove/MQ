package com.fly.mq.mqclient;

import com.fly.mq.common.*;

import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Connection {
    private Socket socket;
    // 使用这个 map 来管理多个 Channel 对象
    private ConcurrentHashMap<String, Channel> channelMap = new ConcurrentHashMap<>();

    private InputStream inputStream;
    private OutputStream outputStream;
    private DataInputStream dataInputStream;
    private DataOutputStream dataOutputStream;

    // 用于执行消费者自己的回调函数
    private ExecutorService callbackExecutor;

    public Connection(String host, int port) throws IOException {
        socket = new Socket(host,port);
        inputStream = socket.getInputStream();
        outputStream = socket.getOutputStream();
        dataInputStream = new DataInputStream(inputStream);
        dataOutputStream = new DataOutputStream(outputStream);

        callbackExecutor = Executors.newFixedThreadPool(4);

        // 创建一个扫描线程不停的从 socket 中读取响应数据交给对应的 channel 处理
        Thread t = new Thread(() -> {
            try {
                while (!socket.isClosed()) {
                    Response response = readResponse();

                    dispatchResponse(response);
                }
            }catch (SocketException e ){
                // 忽略此异常
                System.out.println("[Connection] 连接正常断开");
            }catch (IOException | ClassNotFoundException e){
                System.out.println("[Connection] 连接异常断开");
                e.printStackTrace();
            }
        });
        t.start();
    }

    // 关闭 Connection 释放资源
    public void close(){
        try{
            callbackExecutor.shutdownNow();
            channelMap.clear();
            inputStream.close();
            outputStream.close();
            socket.close();
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    // 使用这个方法分别处理 当前响应是一个针对控制请求的响应 还是服务器推送消息的响应
    private void dispatchResponse(Response response) throws IOException, ClassNotFoundException {
        if(response.getType() == 0xc){
            // 服务器推送的数据消息
            SubScribeReturns scribeReturns = (SubScribeReturns) BinaryTool.fromBytes(response.getPayload());
            Channel channel = channelMap.get(scribeReturns.getChannelId());
            if(channel == null){
                throw new MQException("[Connection] 对应的 Channel 在客户端中不存在");
            }
            // 将回调方法交给线程池
            callbackExecutor.submit(() -> {
                try {
                    channel.getConsumer().handleDelivery(scribeReturns.getConsumerTag(),scribeReturns.getProperties(),scribeReturns.getBody());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }else{
            // 控制请求的响应数据
            BaseReturns baseReturns = (BaseReturns) BinaryTool.fromBytes(response.getPayload());
            Channel channel = channelMap.get(baseReturns.getChannelId());
            if(channel == null){
                throw new MQException("[Connection] 对应的 Channel 在客户端中不存在");
            }
            channel.putReturns(baseReturns);
        }
    }

    // 发送请求
    public void writeRequest(Request request) throws IOException {
        dataOutputStream.writeInt(request.getType());
        dataOutputStream.writeInt(request.getLength());
        dataOutputStream.write(request.getPayload());
        dataOutputStream.flush();

        System.out.println("[Connection] 发送请求! type=" + request.getType() + ", length=" + request.getLength());
    }

    // 读取响应
    public Response readResponse() throws IOException {
        try {
            Response response = new Response();
            response.setType(dataInputStream.readInt());
            response.setLength(dataInputStream.readInt());
            byte[] payload = new byte[response.getLength()];

            dataInputStream.readFully(payload);
            response.setPayload(payload);
            System.out.println("[Connection] 收到响应! type=" + response.getType() + ", length=" + response.getLength());
            return response;
        }catch (EOFException e){
            throw new IOException("读取数据出错");
        }
    }

    // 创建出一个 channel
    public Channel createChannel() throws IOException {
        String channelId = "C-" + UUID.randomUUID().toString();
        Channel channel = new Channel(channelId,this);
        channelMap.put(channelId,channel);
        boolean ok = channel.createChannel();
        if(!ok){
            // 说明从服务器创建channel失败
            channelMap.remove(channelId);
            return null;
        }
        return channel;
    }
}
