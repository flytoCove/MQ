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
        // 先切分两个 key
        String[] bindingTokens = binding.getBindingKey().split("\\.");
        String[] routingTokens = message.getRoutingKey().split("\\.");
        // 将两个下标封分别指向 tokens 起始位置
        int bindingIndex = 0, routingIndex = 0;
        while(bindingIndex < bindingTokens.length && routingIndex < routingTokens.length){
            // 如果是 * 则直接跳过这个位置进行下一个位置的匹配
            if(bindingTokens[bindingIndex].equals("*")){
                bindingIndex++;
                routingIndex++;
                continue;
            }

            // 如果是 # 判断一下是不是最后一个位置 如果是最后一个则一定是匹配成功的
            // #.ccc                aaa.bbb.ccc         true
            else if(bindingTokens[bindingIndex].equals("#")){
                bindingIndex++;
                if(bindingIndex == bindingTokens.length){
                    return true;
                }

                // # 后面如果还有 则拿着这个内容去 routingTokens 中往后找 找到对应的位置
                // findNextMatch 用来查找 bindingTokens[bindingIndex] 这部分在 routingTokens 中对应的位置 返回对应的下标
                // 如果没找到 返回 -1
                routingIndex = findNextMatch(routingTokens,routingIndex,bindingTokens[bindingIndex]);
                if(routingIndex == -1){
                    return false;
                }

                // 如果找到则继续往下匹配
                bindingIndex++;
                routingIndex++;
            }
            else{
                // 如果是普通字符 要求完全相等 否则匹配失败
                if(!bindingTokens[bindingIndex].equals(routingTokens[routingIndex])){
                    return false;
                }
                bindingIndex++;
                routingIndex++;
            }
        }
        // 判断双方是否同时到达末尾
        return bindingIndex == bindingTokens.length && routingIndex == routingTokens.length;
    }

    private static int findNextMatch(String[] routingTokens, int routingIndex, String bindingToken) {
        for(int i = routingIndex; i < routingTokens.length; i++){
            if(routingTokens[i].equals(bindingToken)){
                return i;
            }
        }
        return -1;
    }
}
