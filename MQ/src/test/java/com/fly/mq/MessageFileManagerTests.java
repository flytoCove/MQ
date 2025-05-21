package com.fly.mq;

import com.fly.mq.mqserver.dao.MassageFileManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;
import java.io.IOException;

@SpringBootTest
public class MessageFileManagerTests {
    MassageFileManager massageFileManager = new MassageFileManager();
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
        MassageFileManager.Stat stat = new MassageFileManager.Stat();
        stat.totalCount = 100;
        stat.validCount = 50;

        // 此处使用反射拿到私有方法来进行测试
        // 使用 spring 封装好的反射工具类
        ReflectionTestUtils.invokeMethod(massageFileManager, "writeStat",queueName1, stat);
        // 写入完毕在调用读取验证数据是否一致
        MassageFileManager.Stat newStat = ReflectionTestUtils.invokeMethod(massageFileManager, "readStat",queueName1);

        assert newStat != null;
        Assertions.assertEquals(100, newStat.totalCount);
        Assertions.assertEquals(50, newStat.validCount);
    }
}
