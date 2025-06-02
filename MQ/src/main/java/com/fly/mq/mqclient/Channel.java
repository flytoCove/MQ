package com.fly.mq.mqclient;

import com.fly.mq.common.*;
import com.fly.mq.mqserver.core.BasicProperties;
import com.fly.mq.mqserver.core.ExchangeType;
import lombok.Getter;
import lombok.Setter;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Getter
@Setter
public class Channel {
    private String channelId;
    // 记录当前 channel 属于哪个连接
    private Connection connection;
    // 用来存储后续从服务器获取到的响应
    private ConcurrentHashMap<String, BaseReturns> basicReturnsMap = new ConcurrentHashMap<>();
    // 消费者的回调. 对于消息响应, 调用这个回调处理消息
    private Consumer consumer;
    //private final ConcurrentHashMap<String, Consumer> consumerMap = new ConcurrentHashMap<>();

    public Channel(String channelId, Connection connection) {
        this.channelId = channelId;
        this.connection = connection;
    }

    // 和服务器交互 创建一个 channel
    public boolean createChannel() throws IOException {
        // 对于创建 channel 来说 payload 即是一个 basicArguments
        BaseArguments baseArguments = new BaseArguments();
        baseArguments.setChannelId(channelId);
        baseArguments.setRid(generateRid());
        byte[] payload = BinaryTool.toBytes(baseArguments);

        // 构造请求
        Request request = new Request();
        request.setType(0x1);
        request.setLength(payload.length);
        request.setPayload(payload);

        // 发送请求
        connection.writeRequest(request);

        // 等待服务器响应
        BaseReturns basicReturns = waitResult(baseArguments.getRid());
        return basicReturns.isOk();
    }

    // 阻塞等待服务器响应
    private BaseReturns waitResult(String rid) {
        BaseReturns basicReturns = null;
        // 循环的去读看看有没有收到服务器的响应
        while((basicReturns = basicReturnsMap.get(rid)) == null) {
            // 如果为空说明还没有收到服务器的响应
            // 需要阻塞等待
            synchronized (this) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        basicReturnsMap.remove(rid);
        return basicReturns;
    }

    // 将服务器响应的数据放入 map 中唤醒所有线程 再让 waitResult 查询是否收到了自己的响应
    public void putReturns(BaseReturns basicReturns) {
        basicReturnsMap.put(basicReturns.getRid(), basicReturns);
        // 当前也不清楚有多少等待的线程 直接唤醒所有的
        synchronized (this) {
            notifyAll();
        }
    }

    private String generateRid(){
        return "R-" + UUID.randomUUID().toString();
    }

    public boolean close() throws IOException {
        BaseArguments basicArguments = new BaseArguments();
        basicArguments.setChannelId(channelId);
        basicArguments.setRid(generateRid());
        byte[] payload = BinaryTool.toBytes(basicArguments);

        // 构造请求
        Request request = new Request();
        request.setType(0x2);
        request.setLength(payload.length);
        request.setPayload(payload);

        // 发送请求
        connection.writeRequest(request);
        // 等待服务器响应
        BaseReturns basicReturns = waitResult(basicArguments.getRid());

        return basicReturns.isOk();
    }

    // 创建交换机
    public boolean exchangeDeclare(String exchangeName, ExchangeType exchangeType, boolean durable, boolean autoDelete, Map<String,Object> arguments) throws IOException {
        ExchangeDeclareArguments exchangeDeclareArguments = new ExchangeDeclareArguments();

        exchangeDeclareArguments.setRid(generateRid());
        exchangeDeclareArguments.setChannelId(channelId);
        exchangeDeclareArguments.setExchangeName(exchangeName);
        exchangeDeclareArguments.setExchangeType(exchangeType);
        exchangeDeclareArguments.setDurable(durable);
        exchangeDeclareArguments.setAutoDelete(autoDelete);
        exchangeDeclareArguments.setArguments(arguments);

        byte[] payload = BinaryTool.toBytes(exchangeDeclareArguments);
        Request request = new Request();
        request.setType(0x3);
        request.setLength(payload.length);
        request.setPayload(payload);

        connection.writeRequest(request);
        BaseReturns basicReturns = waitResult(exchangeDeclareArguments.getRid());

        return basicReturns.isOk();
    }

    // 删除交换机
    public boolean exchangeDelete(String exchangeName) throws IOException {
        ExchangeDeleteArguments exchangeDeleteArguments = new ExchangeDeleteArguments();
        exchangeDeleteArguments.setRid(generateRid());
        exchangeDeleteArguments.setChannelId(channelId);
        exchangeDeleteArguments.setExchangeName(exchangeName);
        byte[] payload = BinaryTool.toBytes(exchangeDeleteArguments);

        Request request = new Request();
        request.setType(0x4);
        request.setLength(payload.length);
        request.setPayload(payload);

        connection.writeRequest(request);
        BaseReturns basicReturns = waitResult(exchangeDeleteArguments.getRid());

        return basicReturns.isOk();
    }

    // 创建队列
    public boolean queueDeclare(String queueName, boolean durable, boolean exclusive, boolean autoDelete, Map<String,Object> arguments) throws IOException {
        QueueDeclareArguments queueDeclareArguments = new QueueDeclareArguments();
        queueDeclareArguments.setRid(generateRid());
        queueDeclareArguments.setChannelId(channelId);
        queueDeclareArguments.setQueueName(queueName);
        queueDeclareArguments.setDurable(durable);
        queueDeclareArguments.setExclusive(exclusive);
        queueDeclareArguments.setAutoDelete(autoDelete);
        queueDeclareArguments.setArguments(arguments);
        byte[] payload = BinaryTool.toBytes(queueDeclareArguments);

        Request request = new Request();
        request.setType(0x5);
        request.setLength(payload.length);
        request.setPayload(payload);

        connection.writeRequest(request);
        BaseReturns basicReturns = waitResult(queueDeclareArguments.getRid());

        return basicReturns.isOk();
    }

    // 删除队列
    public boolean queueDelete(String queueName) throws IOException {
        QueueDeleteArguments queueDeleteArguments = new QueueDeleteArguments();
        queueDeleteArguments.setRid(generateRid());
        queueDeleteArguments.setChannelId(channelId);
        queueDeleteArguments.setQueueName(queueName);
        byte[] payload = BinaryTool.toBytes(queueDeleteArguments);

        Request request = new Request();
        request.setType(0x6);
        request.setLength(payload.length);
        request.setPayload(payload);

        connection.writeRequest(request);
        BaseReturns basicReturns = waitResult(queueDeleteArguments.getRid());

        return basicReturns.isOk();
    }

    // 绑定
    public boolean queueBind(String exchangeName, String queueName, String bindingKey) throws IOException {
        QueueBindArguments queueBindArguments = new QueueBindArguments();
        queueBindArguments.setRid(generateRid());
        queueBindArguments.setChannelId(channelId);
        queueBindArguments.setExchangeName(exchangeName);
        queueBindArguments.setQueueName(queueName);
        queueBindArguments.setBindingKey(bindingKey);
        byte[] payload = BinaryTool.toBytes(queueBindArguments);

        Request request = new Request();
        request.setType(0x7);
        request.setLength(payload.length);
        request.setPayload(payload);

        connection.writeRequest(request);
        BaseReturns basicReturns = waitResult(queueBindArguments.getRid());

        return basicReturns.isOk();
    }

    // 解除绑定
    public boolean queueUnbind(String exchangeName, String queueName) throws IOException {
        QueueUnBindArguments queueUnBindArguments = new QueueUnBindArguments();
        queueUnBindArguments.setRid(generateRid());
        queueUnBindArguments.setChannelId(channelId);
        queueUnBindArguments.setExchangeName(exchangeName);
        queueUnBindArguments.setQueueName(queueName);
        byte[] payload = BinaryTool.toBytes(queueUnBindArguments);

        Request request = new Request();
        request.setType(0x8);
        request.setLength(payload.length);
        request.setPayload(payload);

        connection.writeRequest(request);
        BaseReturns basicReturns = waitResult(queueUnBindArguments.getRid());

        return basicReturns.isOk();
    }

    // 发送消息
    public boolean basicPublish(String exchangeName, String routingKey, BasicProperties basicProperties, byte[] body) throws IOException {
        BasicPublishArguments basicPublishArguments = new BasicPublishArguments();
        basicPublishArguments.setRid(generateRid());
        basicPublishArguments.setChannelId(channelId);
        basicPublishArguments.setExchangeName(exchangeName);
        basicPublishArguments.setRoutingKey(routingKey);
        basicPublishArguments.setBasicProperties(basicProperties);
        basicPublishArguments.setBody(body);
        byte[] payload = BinaryTool.toBytes(basicPublishArguments);

        Request request = new Request();
        request.setType(0x9);
        request.setLength(payload.length);
        request.setPayload(payload);

        connection.writeRequest(request);
        BaseReturns basicReturns = waitResult(basicPublishArguments.getRid());

        return basicReturns.isOk();
    }

    // 订阅消息
    public boolean basicConsume(String queueName,boolean autoAck, Consumer consumer) throws MQException, IOException {
        // 先设置回调
        if(this.consumer != null) {
            throw new MQException("[basicConsume] consumer is already set");
        }
        this.consumer = consumer;
//        String consumerTag = "CT-" + UUID.randomUUID().toString();
//        // 存储消费者（按consumerTag存储）
//        if (consumerMap.putIfAbsent(consumerTag, consumer) != null) {
//            throw new MQException("Duplicate consumerTag: " + consumerTag);
//        }

        BasicConsumeArguments basicConsumeArguments = new BasicConsumeArguments();
        basicConsumeArguments.setRid(generateRid());
        basicConsumeArguments.setChannelId(channelId);
        basicConsumeArguments.setConsumerTag(channelId); // consumerTag 也设为 channelId
        basicConsumeArguments.setQueueName(queueName);
        basicConsumeArguments.setAutoAck(autoAck);
        byte[] payload = BinaryTool.toBytes(basicConsumeArguments);

        Request request = new Request();
        request.setType(0xa);
        request.setLength(payload.length);
        request.setPayload(payload);

        connection.writeRequest(request);
        BaseReturns basicReturns = waitResult(basicConsumeArguments.getRid());

        return basicReturns.isOk();
    }

    // 手动 ack
    public boolean basicAck(String queueName, String messageId) throws IOException {
        BasicAckArguments basicAckArguments = new BasicAckArguments();
        basicAckArguments.setRid(generateRid());
        basicAckArguments.setChannelId(channelId);
        basicAckArguments.setQueueName(queueName);
        basicAckArguments.setMessageId(messageId);
        byte[] payload = BinaryTool.toBytes(basicAckArguments);

        Request request = new Request();
        request.setType(0xb);
        request.setLength(payload.length);
        request.setPayload(payload);

        connection.writeRequest(request);
        BaseReturns basicReturns = waitResult(basicAckArguments.getRid());

        return basicReturns.isOk();
    }

}
