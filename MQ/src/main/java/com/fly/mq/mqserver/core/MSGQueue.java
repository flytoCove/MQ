package com.fly.mq.mqserver.core;

//为了避免和库里的 Queue 名称混淆 这里取名 MSGQueue => message queue

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

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

    // 这里方便代码内部使用和测试
    public Object getArguments(String key) {
        return arguments.get(key);
    }

    public void setArguments(String key, Object value) {
        arguments.put(key, value);
    }
}
