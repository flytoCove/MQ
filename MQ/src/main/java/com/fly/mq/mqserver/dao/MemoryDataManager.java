package com.fly.mq.mqserver.dao;

import com.fly.mq.common.MQException;
import com.fly.mq.mqserver.core.Binding;
import com.fly.mq.mqserver.core.Exchange;
import com.fly.mq.mqserver.core.MSGQueue;
import com.fly.mq.mqserver.core.Message;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 管理内存中的数据
 */
public class MemoryDataManager {
    // key:exchangeName value:Exchange
    private ConcurrentHashMap<String, Exchange> exchangeMap = new ConcurrentHashMap<>();
    // key:queueName value:MSGQueue
    private ConcurrentHashMap<String, MSGQueue> queueMap = new ConcurrentHashMap<>();
    // 第一个 key:exchangeName 第二个 key:queueName
    private ConcurrentHashMap<String, ConcurrentHashMap<String, Binding>> bindingsMap = new ConcurrentHashMap<>();
    // key:messageId value:Message
    private ConcurrentHashMap<String, Message> messageMap = new ConcurrentHashMap<>();
    // 表示队列和消息之间的关联关系 key:queueName value:是一个 Message 链表
    private ConcurrentHashMap<String, LinkedList<Message>> queueMessageMap = new ConcurrentHashMap<>();
    // 表示未被确认的消息 1.key:queueName 2.key:messageId
    private ConcurrentHashMap<String, ConcurrentHashMap<String, Message>> queueMessageWaitAckMap = new ConcurrentHashMap<>();

    // 针对 Exchange
    public void insertExchange(Exchange exchange) {
        exchangeMap.put(exchange.getName(), exchange);
        System.out.println("[MemoryDataManager] insert exchange: " + exchange.getName());
    }
    public Exchange getExchange(String exchangeName) {
        return exchangeMap.get(exchangeName);
    }
    public void deleteExchange(String exchangeName) {
        exchangeMap.remove(exchangeName);
        System.out.println("[MemoryDataManager] delete exchange: " + exchangeName);
    }

    // 针对 Queue
    public void insertQueue(MSGQueue queue) {
        queueMap.put(queue.getName(), queue);
        System.out.println("[MemoryDataManager] insert queue: " + queue.getName());
    }
    public MSGQueue getQueue(String queueName) {
        return queueMap.get(queueName);
    }
    public void deleteQueue(String queueName) {
        queueMap.remove(queueName);
        System.out.println("[MemoryDataManager] delete queue: " + queueName);
    }


    public void insertBinding(Binding binding) {
//        if(!bindingsMap.containsKey(binding.getExchangeName())) {
//            ConcurrentHashMap<String, Binding> bindingMap = new ConcurrentHashMap<>();
//            bindingMap.put(binding.getExchangeName(), binding);
//        }
        // 先通过 exchangeName 查询是否存在
        ConcurrentHashMap<String, Binding> bindingMap = bindingsMap.computeIfAbsent(binding.getExchangeName(), k -> new ConcurrentHashMap<>());
        // 再通过 queueName 查询
        // 加锁保证线程安全
        synchronized (bindingMap) {
            if (bindingMap.containsKey(binding.getQueueName())) {
                throw new MQException("[MemoryDataManager] binding already exists: exchangeName：" + binding.getExchangeName() +" queueName: "+ binding.getQueueName());
            }
            bindingMap.put(binding.getQueueName(), binding);
        }
        System.out.println("[MemoryDataManager] insert binding: " + binding.getQueueName());
    }

    // 根据 exchangeName 和 queueName 获取唯一的一个绑定
    public Binding getBinding(String exchangeName, String queueName) {
        ConcurrentHashMap<String, Binding> bindingMap = bindingsMap.get(exchangeName);
        if (bindingMap == null) {
            return null;
        }
        return bindingMap.get(queueName);
    }
    // 根据 exchangeName 获取 所有绑定
    public ConcurrentHashMap<String, Binding> getBindings(String exchangeName) {
        return bindingsMap.get(exchangeName);
    }

    public void deleteBinding(Binding binding) {
        ConcurrentHashMap<String, Binding> bindingMap = bindingsMap.get(binding.getExchangeName());
        if (bindingMap == null) {
            throw new MQException("[MemoryDataManager] binding not exists : exchangeName = "+ binding.getExchangeName() +" queueName = "+ binding.getQueueName());
        }

        bindingMap.remove(binding.getQueueName());
        System.out.println("[MemoryDataManager] delete binding: exchangeName = " + binding.getExchangeName() + " queueName: " + binding.getQueueName());
    }

    public void addMessage(Message message) {
        messageMap.put(message.getMessageId(), message);
        System.out.println("[MemoryDataManager] add message: " + message.getMessageId());
    }

    // 从消息中心查询指定消息
    public Message getMessage(String messageId) {
        return messageMap.get(messageId);
    }
    // 从消息中心删除指定消息
    public void removeMessage(String messageId) {
        messageMap.remove(messageId);
        System.out.println("[MemoryDataManager] delete message: " + messageId);
    }


    // 发送消息到指定队列 把消息放到指定队列的数结构中
    public void sendMessage(MSGQueue queue,Message message) {
        // 根据队列的名字找到队列对应的链表
//        LinkedList<Message> messages = queueMessageMap.get(queue.getName());
//        if (messages == null) {
//            messages = new LinkedList<>();
//        }
        LinkedList<Message> messages = queueMessageMap.computeIfAbsent(queue.getName(), k -> new LinkedList<>());
        synchronized (message) {
            messages.add(message);
        }
        // 即使重复插入也没影响
        addMessage(message);
        System.out.println("[MemoryDataManager] send message: " + message.getMessageId());
    }

    // 从指定队列取出消息
    public Message pollMessage(String queueName) {
        // 根据队列名查找一下对应的消息链表
        LinkedList<Message> messages = queueMessageMap.get(queueName);
        if(messages == null) {
            return null;
        }
        synchronized (messages) {

            if (messages.isEmpty()) {
                return null;
            }
            // 删除第一个元素
            return messages.removeFirst();
        }
    }

    // 获取指定队列中的消息个数
    public int getMessageCount(String queueName) {
        LinkedList<Message> messages = queueMessageMap.get(queueName);
        if(messages == null) {
            return 0;
        }

        synchronized (messages) {
            return messages.size();
        }
    }

    // 添加未确认消息
    public void addMessageWaitAck(String queueName, Message message) {
        ConcurrentHashMap<String, Message> messageMap = queueMessageWaitAckMap.computeIfAbsent(queueName, k -> new ConcurrentHashMap<>());
        messageMap.put(message.getMessageId(), message);
        System.out.println("[MemoryDataManager] add message wait ack: " + message.getMessageId());
    }

    // 删除未确认消息
    public void removeMessageWaitAck(String queueName, String messageId) {
        ConcurrentHashMap<String, Message> messageMap = queueMessageWaitAckMap.get(queueName);
        if(messageMap == null) {
            return ;
        }

        messageMap.remove(messageId);
        System.out.println("[MemoryDataManager] delete message wait ack: " + messageId);
    }
    // 获取指定未确认消息
    public Message pollMessageWaitAck(String queueName, String messageId) {
        ConcurrentHashMap<String, Message> messageMap = queueMessageWaitAckMap.get(queueName);
        if(messageMap == null) {
            return null;
        }
        return messageMap.get(messageId);
    }

    /**
     * 从硬盘上读取数据（将硬盘上持久化存储的数据恢复到内存中）
     */
    public void recovery(DiskDataManager diskDataCenter) throws IOException, ClassNotFoundException {
        // 清空之前的数据
        exchangeMap.clear();
        queueMap.clear();
        bindingsMap.clear();
        messageMap.clear();
        queueMessageMap.clear();

        // 1.恢复所有的交换机数据
        List<Exchange> exchanges = diskDataCenter.getAllExchanges();
        exchanges.forEach(exchange -> {
            exchangeMap.put(exchange.getName(), exchange);
        });
        // 2.恢复所有的队列数据
        List<MSGQueue> queues = diskDataCenter.getAllQueues();
        queues.forEach(queue -> {
            queueMap.put(queue.getName(), queue);
        });
        // 3.恢复所有的绑定数据
        List<Binding> bindings = diskDataCenter.getAllBindings();
        bindings.forEach(binding -> {
            ConcurrentHashMap<String, Binding> bindingMap = bindingsMap.computeIfAbsent(binding.getExchangeName(), k -> new ConcurrentHashMap<>());
            bindingMap.put(binding.getQueueName(), binding);
        });
        // 4.恢复所有的消息数据
        // 遍历所有的队列 根据队列的名字 获取到所有的消息
        for(MSGQueue queue : queues) {
            LinkedList<Message> messages = diskDataCenter.loadAllMessageFromQueue(queue.getName());
            queueMessageMap.put(queue.getName(),messages);
            for(Message message : messages) {
                messageMap.put(message.getMessageId(), message);
            }
        }
        // 针对未被确认的消息 一旦在等待 ack 的过程中服务器重启了 此时未被确认的消息就恢复成在队列中"未被取走"的消息
        // 即在硬盘上存储的时候就当作是"未被取走"的消息
    }
}
