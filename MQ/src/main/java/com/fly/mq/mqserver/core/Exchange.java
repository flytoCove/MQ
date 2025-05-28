package com.fly.mq.mqserver.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

/**
 * 表示交换机
 */
public class Exchange {
    // 表示交换机的唯一标识
    @Setter
    @Getter
    private String name;
    //表示交换机类型
    @Setter
    @Getter
    private ExchangeType type = ExchangeType.DIRECT;
    //表示该交换机是否需要持久化存储
    @Setter
    @Getter
    private boolean durable = false;
    //表示是否自动删除
    @Setter
    @Getter
    private boolean autoDelete = false;
    //表示创建交换机是指定的一些额外的选项
    private Map<String, Object> arguments = new HashMap<>();

    // 用于数据库交互
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
