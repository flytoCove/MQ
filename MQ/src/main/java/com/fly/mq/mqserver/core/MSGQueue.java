package com.fly.mq.mqserver.core;

//为了避免和库里的 Queue 名称混淆 这里取名 MSGQueue => message queue

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fly.mq.common.ConsumerEnv;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 表示存储消的队列
 */

public class MSGQueue {
    @Setter
    @Getter
    // 表示队列的唯一标识
    private String name;
    @Setter
    @Getter
    //表示该队列是否需要持久化存储
    private boolean durable = false;
    @Setter
    @Getter
    //没人使用是否自动删除
    private boolean autoDelete = false;
    @Setter
    @Getter
    //为 true 表示这个队列只能被一个消费者使用
    private boolean exclusive = false;
    //表示创建交换机是指定的一些额外的选项
    private Map<String, Object> arguments = new HashMap<>();

    // 表示当前队列都有那些消费者订阅了
    private List<ConsumerEnv> consumerEnvList = new ArrayList<>();

    // AtomicInteger 用于在多线程环境下进行原子操作的整数操作
    // 记录当前取到了那个消费者方便实现轮询
    private AtomicInteger consumerSeq = new AtomicInteger(0);

    // 添加一个订阅者
    public void addConsumerEnv(ConsumerEnv consumerEnv){
        consumerEnvList.add(consumerEnv);
    }

    // 删除一个订阅者 TODO

    // 选择一个订阅者处理当前的消息（轮询的方式）
    public ConsumerEnv chooseConsumerEnv(){
       if(consumerEnvList.isEmpty()){
           return null;
       }
       // 记录当前元素的下标
       int index = consumerSeq.get() % consumerEnvList.size();
       consumerSeq.getAndIncrement();
       return consumerEnvList.get(index);
    }


    // 数据库存储不支持 Map 类型的数据 这里通过 get set 方法进行转换
    public String getArguments() {
        // 把当前的 arguments 参数从 Map 转换成 String(JSON)
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(arguments);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return "{}";
    }

    // 从数据库读数据之后构造 Exchange 对象自动调用
    public void setArguments(String argumentsJson) {
        // 把当前的 argumentsJson 参数从 String(JSON) 转换成 Map
        ObjectMapper mapper = new ObjectMapper();
        try {
            this.arguments = mapper.readValue(argumentsJson, new TypeReference<HashMap<String,Object>>() {});
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    public void setArguments(Map<String, Object> arguments) {
        this.arguments = arguments;
    }

    // 这里方便代码内部使用和测试
    public Object getArguments(String key) {
        return arguments.get(key);
    }

    public void setArguments(String key, Object value) {
        arguments.put(key, value);
    }
}
