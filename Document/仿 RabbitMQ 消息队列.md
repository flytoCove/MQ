# 仿 RabbitMQ 消息队列

## 需求分析

核⼼概念：

-  ⽣产者 (Producer) 
- 消费者 (Consumer)  
- 中间⼈ (Broker) 
- 发布 (Publish) 
-  订阅 (Subscribe)

![image-20250513214041168](C:\Users\wyf\AppData\Roaming\Typora\typora-user-images\image-20250513214041168.png)

虚拟机 (VirtualHost): 类似于 MySQL 的 "database", 是⼀个逻辑上的集合. ⼀个 BrokerServer 上可 以存在多个 VirtualHost. 

 交换机 (Exchange): 生产者把消息先发送到 Broker 的 Exchange 上. 再根据不同的规则, 把消息转发 给不同的 Queue. 

 队列 (Queue): 真正用来存储消息的部分. 每个消费者决定自己从哪个 Queue 上读取消息. 

绑定 (Binding): Exchange 和 Queue 之间的关联关系. Exchange 和 Queue 可以理解成 "多对多" 关 系. 使⽤⼀个关联表就可以把这两个概念联系起来.

消息 (Message): 传递的内容. 所谓的 Exchange 和 Queue 可以理解成 "多对多" 关系, 和数据库中的 "多对多" ⼀样. 意思是: ⼀个 Exchange 可以绑定多个 Queue (可以向多个 Queue 中转发消息). ⼀个 Queue 也可以被多个 Exchange 绑定 (⼀个 Queue 中的消息可以来⾃于多个 Exchange).

![image-20250513214307056](C:\Users\wyf\AppData\Roaming\Typora\typora-user-images\image-20250513214307056.png)

核心 API：

1. 创建队列 (queueDeclare) 
2. 销毁队列 (queueDelete) 
3. 创建交换机 (exchangeDeclare) 
4. 销毁交换机 (exchangeDelete) 
5. 创建绑定 (queueBind) 
6. 解除绑定 (queueUnbind) 
7. 发布消息 (basicPublish) 
8. 订阅消息 (basicConsume) 
9. 确认消息 (basicAck)

Producer 和 Consumer 通过⽹络的⽅式, 远程调⽤这些 API, 实现 ⽣产者消费者模型

交换机类型 (Exchange Type) （对于 RabbitMQ 来说, 主要⽀持四种交换机类型）：

- Direct  ： 生产者发送消息时, 直接指定被该交换机绑定的队列名
- Fanout ：生产者发送的消息会被复制到该交换机的所有队列中
- Topic  ：绑定队列到交换机上时, 指定⼀个字符串为 bindingKey. 发送消息指定⼀个字符串为 routingKey. 当 routingKey 和 bindingKey 满⾜⼀定的匹配条件的时候, 则把消息投递到指定队列.
- Header：（暂不实现）

⽹络通信 ：

⽣产者和消费者都是客户端程序, broker 则是作为服务器. 通过网络进行通信. 在网络通信的过程中, 客户端部分要提供对应的 api, 来实现对服务器的操作.

1. 创建 Connection
2. 关闭 Connection
3. 创建 Channel
4. 关闭 Channel
5. 创建队列 (queueDeclare)
6. 销毁队列 (queueDelete)
7. 创建交换机 (exchangeDeclare)
8. 销毁交换机 (exchangeDelete)
9. 创建绑定 (queueBind)
10. 解除绑定 (queueUnbind)
11. 发布消息 (basicPublish)
12. 订阅消息 (basicConsume)
13. 确认消息 (basicAck)

Connection 对应⼀个 TCP 连接.
Channel 则是 Connection 中的逻辑通道.
⼀个 Connection 中可以包含多个 Channel.
Channel 和 Channel 之间的数据是独立的. 不会相互干扰.
这样的设定主要是为了能够更好的复⽤ TCP 连接, 达到⻓连接的效果, 避免频繁的创建关闭 TCP 连接

消息应答：

被消费的消息, 需要进⾏应答.

应答模式分成两种.

- ⾃动应答: 消费者只要消费了消息, 就算应答完毕了. Broker 直接删除这个消息.
- ⼿动应答: 消费者⼿动调⽤应答接⼝, Broker 收到应答请求之后, 才真正删除这个消息

## 模块划分



## 项目创建



