package com.fly.mq.mqserver.core;

import com.fly.mq.common.MQException;

/**
 * 实现交换机的转发规则
 * 验证 bindingKey 是否合法
 */
public class Router {
    public static boolean checkBindingKey(String bindingKey) {
        // bindingKey 构造规则
        // 1.数字 字母 下划线
        // 2.使用 . 分割成若干个部分
        // 3.允许使用 * 或者 # 作为通配符 但是只能作为独立的分段存在
        if(bindingKey == null || bindingKey.isEmpty()){
            // 使用 fanout 或者 topic 交换机的时候 bindingKey 用不上
            return true;
        }
        for(int i = 0; i < bindingKey.length(); i++){
            char c = bindingKey.charAt(i);
            if(Character.isLetterOrDigit(c)){
                continue;
            }
            if(c == '_' || c == '.' || c == '*' || c == '#'){
                continue;
            }

            return false;
        }
        // 检查 * 或者 # 是否是独立的部分
        // aaa.*.ccc 合法    aaa.*b.ccc 非法
        String[] words = bindingKey.split("\\.");
        for(String word : words){
            // 如果分割后长度大于 1 的情况下包含 * 或者 # 说明非法
            if(word.length() > 1 && (word.contains("*") || word.contains("#"))){
                return false;
            }
        }
        // 通配符之间的相邻关系  此处约定（这么约定单纯是为了方便实现）
        // 1.aaa.#.#.bbb -> 非法
        // 2.aaa.*.#.bbb -> 非法
        // 3.aaa.#.*.bbb -> 非法
        // 4.aaa.*.*.bbb -> 合法
        for(int i = 0; i < words.length - 1; i++){
            // 连续两个 #
            if(words[i].equals("#") && words[i+1].equals("#")){
                return false;
            }
            if(words[i].equals("*") && words[i+1].equals("#")){
                return false;
            }
            if(words[i].equals("#") && words[i+1].equals("*")){
                return false;
            }
        }

        return true;
    }

    public static boolean checkRoutingKey(String routingKey) {
        // routingKey 构造规则
        // 1.数字 字母 下划线
        // 2.使用 . 分割成若干个部分
        if(routingKey.isEmpty()){
            // 空字符串 -> 合法 fanout 交换机 routingKey 用不到 可以是 ""
            return true;
        }
        for(int i = 0; i < routingKey.length(); i++){
            char ch = routingKey.charAt(i);
            // 判断是否是字母或数字 (a-z, A-Z, 0-9)
            if(Character.isLetterOrDigit(ch)){
                continue;
            }

            if(ch == '_' || ch == '.'){
                continue;
            }

            return false;
        }

        return true;
    }

    // 用来判断该消息是否需要转发给这个队列
    public static boolean rout(ExchangeType exchangeType, Binding binding, Message message) throws MQException {
        // 根据 ExchangeType 选择不同的转发规则
        if(exchangeType == ExchangeType.FANOUT) {
            return true;
        }else if(exchangeType == ExchangeType.TOPIC) {
            return routTopic(binding,message);
        }else{
            throw new MQException("[Router] Invalid exchange type");
        }
    }
    public static boolean routTopic(Binding binding, Message message) throws MQException {

        return true;
    }
}
