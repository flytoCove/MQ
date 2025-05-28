package com.fly.mq.mqserver;

import com.fly.mq.common.MQException;
import com.fly.mq.mqserver.core.*;
import com.fly.mq.mqserver.dao.DataBaseManager;
import com.fly.mq.mqserver.dao.DiskDataManager;
import com.fly.mq.mqserver.dao.MemoryDataManager;
import lombok.Getter;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 虚拟主机
 * 每个虚拟主机下面都管理着自己的 交换机 队列 绑定 消息 数据
 * 通过 API 供上层使用
 */
public class VirtualHost {
    @Getter
    private String virtualHostName;
    @Getter
    private DiskDataManager diskDataManager = new DiskDataManager();
    @Getter
    private MemoryDataManager memoryDataManager = new MemoryDataManager();
    // 交换机锁对象
    private final Object exchangeLocker = new Object();
    // 队列锁对象
    private final Object queueLocker = new Object();

    public VirtualHost(String name) {
        this.virtualHostName = name;

        // 此处 memoryDataManager 不需要额外初始化操作
        // 针对 DiskDataManager 需要初始化操作 建库建表 及恢复硬盘数据
        diskDataManager.init();
        try {
            memoryDataManager.recovery(diskDataManager);
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            System.out.println("[VirtualHost] Could not be recovery");
        }
    }

    // 创建交换机
    public boolean exchangeDeclare(String exchangeName, ExchangeType exchangeType, boolean durable, boolean autoDelete, Map<String,Object> arguments) {
        // 将交换机的名字加上虚拟主机名的前缀 以此来确定交换机的归属
        exchangeName = virtualHostName + exchangeName;
        try{
            synchronized (exchangeLocker) {
                // 1.判断交换机是否存在
                Exchange existsExchange = memoryDataManager.getExchange(exchangeName);
                if (existsExchange != null) {
                    System.out.println("[VirtualHost] Exchange " + exchangeName + " already exists");
                    return true;
                }

                // 2.创建交换机
                Exchange exchange = new Exchange();
                exchange.setName(exchangeName);
                exchange.setType(exchangeType);
                exchange.setDurable(durable);
                exchange.setAutoDelete(autoDelete);
                exchange.setArguments(arguments);

                // 3.将交换机数据写入硬盘
                if (durable) {
                    diskDataManager.insertExchange(exchange);
                }

                // 4.将交换机写入内存
                memoryDataManager.insertExchange(exchange);
                System.out.println("[VirtualHost] Exchange " + exchangeName + " created");
            }
            return true;
        }catch (Exception e){
            System.out.println("[VirtualHost] Exchange "+exchangeName+" create failed");
            e.printStackTrace();
            return false;
        }
    }

    // 删除交换机
    public boolean exchangeDelete(String exchangeName){
        exchangeName = virtualHostName + exchangeName;
        try{
            synchronized (exchangeLocker) {

                // 1.先找到对应的交换机
                Exchange toDelete = memoryDataManager.getExchange(exchangeName);
                if (toDelete == null) {
                    throw new MQException("[VirtualHost] Exchange " + exchangeName + " does not exist");
                }

                // 2.删除硬盘上的数据
                if (toDelete.isDurable()) {
                    diskDataManager.deleteExchange(exchangeName);
                }

                // 3.删除硬盘上的数据
                memoryDataManager.deleteExchange(exchangeName);
                System.out.println("[VirtualHost] Exchange " + exchangeName + " deleted");
            }
            return true;

        }catch (Exception e){
            System.out.println("[VirtualHost] Exchange "+exchangeName+" delete failed");
            e.printStackTrace();
            return false;
        }
    }

    public boolean queueDeclare(String queueName, boolean durable, boolean exclusive, boolean autoDelete, Map<String,Object> arguments) {
        queueName = virtualHostName + queueName;
        try{
            synchronized (queueLocker) {
                // 1.判断队列是否存在
                MSGQueue existsQueue = memoryDataManager.getQueue(queueName);
                if (existsQueue != null) {
                    throw new MQException("[VirtualHost] Queue " + queueName + " already exists");
                }

                MSGQueue queue = new MSGQueue();
                queue.setName(queueName);
                queue.setDurable(durable);
                queue.setExclusive(exclusive);
                queue.setAutoDelete(autoDelete);
                queue.setArguments(arguments);

                if (durable) {
                    diskDataManager.insertQueue(queue);
                }
                memoryDataManager.insertQueue(queue);
                System.out.println("[VirtualHost] Queue " + queueName + " created");
            }
            return true;
        }catch (Exception e){
            System.out.println("[VirtualHost] Queue "+queueName+" create failed");
            e.printStackTrace();
            return false;
        }
    }

    // 删除队列
    public boolean queueDelete(String queueName) {
        queueName = virtualHostName + queueName;
        try {
            synchronized (queueLocker) {
                MSGQueue queue = memoryDataManager.getQueue(queueName);
                if (queue == null) {
                    throw new MQException("[VirtualHost] Queue " + queueName + " does not exist");
                }
                if (queue.isDurable()) {
                    diskDataManager.deleteQueue(queueName);
                }
                memoryDataManager.deleteQueue(queueName);
                System.out.println("[VirtualHost] Queue " + queueName + " deleted");
            }
            return true;
        }catch (Exception e){
            System.out.println("[VirtualHost] Queue " + queueName + " delete failed");
            e.printStackTrace();
            return false;
        }
    }


    // 创建绑定
    public boolean queueBind(String exchangeName, String queueName, String bindingKey) {
        exchangeName = virtualHostName + exchangeName;
        queueName = virtualHostName + queueName;
        try{
            synchronized (exchangeLocker) {
                synchronized (queueLocker) {
                    Binding existsBinding = memoryDataManager.getBinding(exchangeName, queueName);
                    if (existsBinding != null) {
                        throw new MQException("[VirtualHost] Binding already exists " + exchangeName + " " + queueName);
                    }

                    // 验证绑定是否存在
                    if (!Router.checkBindingKey(bindingKey)) {
                        throw new MQException("[VirtualHost] Bindingkey 非法 " + bindingKey);
                    }

                    // 创建绑定对象
                    Binding binding = new Binding();
                    binding.setExchangeName(exchangeName);
                    binding.setQueueName(queueName);
                    binding.setBindingKey(bindingKey);

                    // 检查对应的交换机和队列是否存在
                    MSGQueue queue = memoryDataManager.getQueue(queueName);
                    if (queue == null) {
                        throw new MQException("[VirtualHost] Queue " + queueName + " does not exist");
                    }
                    Exchange exchange = memoryDataManager.getExchange(exchangeName);
                    if (exchange == null) {
                        throw new MQException("[VirtualHost] Exchange " + exchangeName + " does not exist");
                    }

                    // 写入硬盘
                    if (queue.isDurable() && exchange.isDurable()) {
                        diskDataManager.insertBinding(binding);
                    }
                    memoryDataManager.insertBinding(binding);
                }
            }
            System.out.println("[VirtualHost] Binding "+bindingKey+" created");
            return true;
        }catch (Exception e){
            System.out.println("[VirtualHost] Binding "+bindingKey+" create failed");
            e.printStackTrace();
            return false;
        }
    }

    // 解除绑定
    public boolean queueUnbind(String exchangeName, String queueName) {
        exchangeName = virtualHostName + exchangeName;
        queueName = virtualHostName + queueName;
        try{
            synchronized (exchangeLocker) {
                synchronized (queueLocker) {
                    Binding binding = memoryDataManager.getBinding(exchangeName, queueName);
                    if (binding == null) {
                        throw new MQException("[VirtualHost] Binding " + queueName + " does not exist");
                    }

                    // 获取队列和交换机是否存在
//            MSGQueue queue = memoryDataManager.getQueue(queueName);
//            if(queue == null){
//                throw new MQException("[VirtualHost] Queue " + queueName + " does not exist");
//            }
//            Exchange exchange = memoryDataManager.getExchange(exchangeName);
//            if(exchange == null){
//                throw new MQException("[VirtualHost] Exchange " + exchangeName + " does not exist");
//            }

                    // 无论是否持久化都删除一次
                    diskDataManager.deleteBinding(binding);

                    memoryDataManager.deleteBinding(binding);
                }
            }

            System.out.println("[VirtualHost] Binding deleted");
            return true;
        }catch (Exception e){
            System.out.println("[VirtualHost] Binding " + queueName + " delete failed");
            e.printStackTrace();
            return false;
        }
    }

    // 发送消息到指定交换机/队列
    public boolean basicPublish(String exchangeName, String routingKey, BasicProperties basicProperties,byte[] data) {
        try {
            exchangeName = virtualHostName + exchangeName;
            if (!Router.checkRoutingKey(routingKey)) {
                throw new MQException("[VirtualHost] routingKey 非法 routingKey: " + routingKey);
            }
            Exchange exchange = memoryDataManager.getExchange(exchangeName);
            if (exchange == null) {
                throw new MQException("[VirtualHost] Exchange " + exchangeName + " does not exist");
            }

            // 判断交换机类型
            if(exchange.getType() == ExchangeType.DIRECT) {
                // 按照直接交换机的方式进行消息转发
                // 无视绑定 直接用 routingKey 作为队列的名字将消息写入到队列中
                String queueName = virtualHostName + routingKey;
                // 构造消息对象
                Message message = Message.createMessageById(routingKey,basicProperties,data);
                MSGQueue queue = memoryDataManager.getQueue(queueName);
                if (queue == null) {
                    throw new MQException("[VirtualHost] Queue " + queueName + " does not exist");
                }

                sendMessage(queue,message);

            }else{
                // 按照 fanout 和 topic
                // 找打该交换机关联的所有绑定
                ConcurrentHashMap<String, Binding> bindings = memoryDataManager.getBindings(exchangeName);
                for (Map.Entry<String, Binding> entry : bindings.entrySet()) {
                    // 1） 获取到绑定对象 判断对应的队列是否存在
                    Binding binding = entry.getValue();
                    MSGQueue queue = memoryDataManager.getQueue(binding.getQueueName());
                    if (queue == null) {
                        // 希望一个队列不存在 不影响其他队列的消息传输
                        System.out.println("[VirtualHost] Queue not exists "+binding.getQueueName());
                        continue;
                    }

                    // 构造消息对象
                    Message message = Message.createMessageById(binding.getBindingKey(),basicProperties,data);
                    // 判断此消息是否要发送到该队列
                    // 1) fanout 所有绑定的队列都需要转发
                    // 2) topic 还需要判断 bindingKey 和 routingKey 是否匹配
                    if(!Router.rout(exchange.getType(),binding,message)) {
                        continue;
                    }
                    sendMessage(queue,message);
                }
            }
            return true;
        }catch (Exception e){
            System.out.println("[VirtualHost] Send message failed");
            e.printStackTrace();
            return false;
        }
    }
    public void sendMessage(MSGQueue queue, Message message) throws IOException {
        // 将消息写入内存和硬盘上
        // deliverMode == 1 不持久化 2 持久化
        if(message.getDeliverMode() == 2){
            // 持久化到硬盘
            diskDataManager.sendMessage(queue,message);
        }
        // 写入消息到内存
        memoryDataManager.sendMessage(queue,message);

        // TODO 通知消费者消费消息
    }
}
