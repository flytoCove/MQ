package com.fly.mq;

import com.fly.mq.mqserver.core.MSGQueue;
import com.fly.mq.mqserver.core.Message;
import com.fly.mq.mqserver.dao.MessageFileManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

@SpringBootTest
public class MessageFileManagerTests {
    MessageFileManager massageFileManager = new MessageFileManager();
    // 方法执行前的准备工作
    private static final String queueName1 = "testQueue1";
    private static final String queueName2 = "testQueue2";
    @BeforeEach
    public void setUp() throws IOException {
        // 创建出两个队列 备用
        massageFileManager.createQueueFiles(queueName1);
        massageFileManager.createQueueFiles(queueName2);

    }

    // 收尾工作
    @AfterEach
    public void tearDown() throws IOException {
        // 删除队列
        massageFileManager.destroyQueueFiles(queueName1);
        massageFileManager.destroyQueueFiles(queueName2);
    }

    // 测试队列文件的创建和删除
    @Test
    public void testCreateQueueFiles() throws IOException {

        File queueFile1 = new File("./data/"+queueName1 + "/queue_data.txt");
        File queueFileStat1 = new File("./data/"+queueName1 + "/queue_stat.txt");
        Assertions.assertTrue(queueFile1.isFile());
        Assertions.assertTrue(queueFileStat1.isFile());

        File queueFile2 = new File("./data/"+queueName2 + "/queue_data.txt");
        File queueFileStat2 = new File("./data/"+queueName2 + "/queue_stat.txt");
        Assertions.assertTrue(queueFile2.isFile());
        Assertions.assertTrue(queueFileStat2.isFile());
    }

    // 测试读写统计
    @Test
    public void testReadWriteStat() throws IOException {
        MessageFileManager.Stat stat = new MessageFileManager.Stat();
        stat.totalCount = 100;
        stat.validCount = 50;

        // 此处使用反射拿到私有方法来进行测试
        // 使用 spring 封装好的反射工具类
        ReflectionTestUtils.invokeMethod(massageFileManager, "writeStat",queueName1, stat);
        // 写入完毕在调用读取验证数据是否一致
        MessageFileManager.Stat newStat = ReflectionTestUtils.invokeMethod(massageFileManager, "readStat",queueName1);

        assert newStat != null;
        Assertions.assertEquals(100, newStat.totalCount);
        Assertions.assertEquals(50, newStat.validCount);
    }

    private MSGQueue createTestQueue(String queueName) throws IOException {
        MSGQueue queue = new MSGQueue();
        queue.setName(queueName);
        queue.setDurable(true);
        queue.setDurable(false);
        queue.setExclusive(false);
        return queue;
    }

    private Message createTestMessage(String content) throws IOException {
        Message message = Message.createMessageById("testRoutingKey",null,content.getBytes());
        return message;
    }

    @Test
    public void testSendMessage() throws IOException, ClassNotFoundException {
        // 构造出雄消息和队列
        Message message = createTestMessage("TestMessage");
        MSGQueue queue = createTestQueue(queueName1);

        massageFileManager.sendMessage(queue, message);
        // 检查 stat 文件
        MessageFileManager.Stat stat = ReflectionTestUtils.invokeMethod(massageFileManager, "readStat",queue.getName());
        Assertions.assertEquals(1, stat.totalCount);
        Assertions.assertEquals(1, stat.validCount);

        // 检查 data 文件
        LinkedList<Message> messages = massageFileManager.loadAllMessageFromQueue(queueName1);
        Message msg = messages.get(0);
        Assertions.assertEquals(1, messages.size());
        Assertions.assertEquals(message.getMessageId(), msg.getMessageId());
        Assertions.assertEquals(message.getRoutingKey(), msg.getRoutingKey());
        Assertions.assertEquals(message.getDeliverMode(), msg.getDeliverMode());
        Assertions.assertArrayEquals(message.getBody(), msg.getBody());

        System.out.println("Msg: " + msg);
    }

    @Test
    public void testLoadAllMessageFromQueue() throws IOException, ClassNotFoundException {
        MSGQueue queue = createTestQueue(queueName1);
        LinkedList<Message> expectedMessages = new LinkedList<>();
        for (int i = 0; i < 100; i++) {
            Message msg = createTestMessage("TestMessage" + i);
            massageFileManager.sendMessage(queue, msg);
            expectedMessages.add(msg);
        }

        // 读取所有消息
        LinkedList<Message> actualMessages = massageFileManager.loadAllMessageFromQueue(queueName1);
        Assertions.assertEquals(expectedMessages.size(), actualMessages.size());
        for (int i = 0; i < expectedMessages.size(); i++) {
            Message msg = expectedMessages.get(i);
            Message actualMsg = actualMessages.get(i);
            System.out.println("expectedMsg: " + msg);
            System.out.println("actualMsg: " + actualMsg);
            Assertions.assertEquals(msg.getMessageId(), actualMsg.getMessageId());
            Assertions.assertEquals(msg.getRoutingKey(), actualMsg.getRoutingKey());
            Assertions.assertEquals(msg.getDeliverMode(), actualMsg.getDeliverMode());
            Assertions.assertArrayEquals(msg.getBody(), actualMsg.getBody());
            //Assertions.assertEquals(msg, actualMsg);
            Assertions.assertEquals(0x1, actualMsg.getIsValid());
        }
    }

    @Test
    public void testDeleteMessage() throws IOException, ClassNotFoundException {
        MSGQueue queue = createTestQueue(queueName1);
        List<Message> expectedMsg = new LinkedList<>();
        for (int i = 0; i < 10; i++) {
            Message message = createTestMessage("TestMessage" + i);
            massageFileManager.sendMessage(queue, message);
            expectedMsg.add(message);

        }

        // 这里删掉后三条消息
        massageFileManager.deleteMessage(queue,expectedMsg.get(7));
        massageFileManager.deleteMessage(queue,expectedMsg.get(8));
        massageFileManager.deleteMessage(queue,expectedMsg.get(9));

        // 读取所有消息
        LinkedList<Message> actualMessages = massageFileManager.loadAllMessageFromQueue(queueName1);
        Assertions.assertEquals(7, actualMessages.size());

        MessageFileManager.Stat stat = ReflectionTestUtils.invokeMethod(massageFileManager, "readStat",queue.getName());
        Assertions.assertEquals(10, stat.totalCount);
        Assertions.assertEquals(7, stat.validCount);

        for (int i = 0; i < actualMessages.size(); i++) {
            Message msg = expectedMsg.get(i);
            Message actualMsg = actualMessages.get(i);
            System.out.println("expectedMsg: " + msg);
            System.out.println("actualMsg: " + actualMsg);
            Assertions.assertEquals(msg.getMessageId(), actualMsg.getMessageId());
            Assertions.assertEquals(msg.getRoutingKey(), actualMsg.getRoutingKey());
            Assertions.assertEquals(msg.getDeliverMode(), actualMsg.getDeliverMode());
            Assertions.assertArrayEquals(msg.getBody(), actualMsg.getBody());
            //Assertions.assertEquals(msg, actualMsg);
            Assertions.assertEquals(0x1, actualMsg.getIsValid());
        }
    }

    @Test
    public void testGC() throws IOException, ClassNotFoundException {
        // 写入 100 条消息 删掉一半 在手动调用 gc 测试 再查看文件大小是否减小
        MSGQueue queue = createTestQueue(queueName1);
        LinkedList<Message> expectedMessages = new LinkedList<>();
        for (int i = 0; i < 100; i++) {
            Message message = createTestMessage("testMessage" + i);
            massageFileManager.sendMessage(queue, message);
            expectedMessages.add(message);
        }

        File beforeGCFile = new File("./data/"+queueName1+"/queue_data.txt");
        long beforeGCLength = beforeGCFile.length();

        // 删除偶数下标的消息
        for (int i = 0; i <expectedMessages.size(); i += 2) {
            massageFileManager.deleteMessage(queue,expectedMessages.get(i));
        }
        // 手动 gc
        massageFileManager.gc(queue);

        // 重新读取文件验证文件内容
        LinkedList<Message> actualMessages = massageFileManager.loadAllMessageFromQueue(queueName1);
        Assertions.assertEquals(50, actualMessages.size());
        for (int i = 0; i < actualMessages.size(); i++) {
            // actual 中的 0 对应 expected 的 1
            // actual 中的 1 对应 expected 的 3
            // actual 中的 2 对应 expected 的 5
            // actual 中的 i 对应 expected 的 2 * i + 1
            Message exceptedMessage = expectedMessages.get(2 * i + 1);
            Message actualMessage = actualMessages.get(i);

            Assertions.assertEquals(exceptedMessage.getMessageId(), actualMessage.getMessageId());
            Assertions.assertEquals(exceptedMessage.getRoutingKey(), actualMessage.getRoutingKey());
            Assertions.assertEquals(exceptedMessage.getDeliverMode(), actualMessage.getDeliverMode());
            Assertions.assertArrayEquals(exceptedMessage.getBody(), actualMessage.getBody());
            //Assertions.assertEquals(exceptedMessage, actualMessage);
            Assertions.assertEquals(0x1, actualMessage.getIsValid());
        }

        File afterGCFile = new File("./data/"+queueName1+"/queue_data.txt");
        long afterGCLength = afterGCFile.length();

        System.out.println("Before GC: " + beforeGCLength);
        System.out.println("After GC: " + afterGCLength);
        Assertions.assertTrue(beforeGCLength > afterGCLength);

    }
}
