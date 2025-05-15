package com.fly.mq.mqserver.dao;

import java.io.*;
import java.util.Scanner;

/**
 * 针对消息管理
 */
public class MassageFileManager {

    // 用来描述该队列消息的统计信息
    static public class Stat {
        // 消息总数
        public int totalCount;
        // 有效消息
        public int validCount;
    }

    // 用来获取指定队列对应的消息所在的文件路径
    private String getQueueDir(String queueName) {
        return "./data/" + queueName;
    }

    // 用来获取该队列的消息数据文件
    private String getQueueDataPath(String queueName) {
        return getQueueDir(queueName) + "queue_data.txt";
    }

    // 用来获取该队列的消息统计文件
    private String getQueueStatPath(String queueName) {
        return getQueueDir(queueName) + "queue_stat.txt";
    }


    // 读取消息统计文件
    private Stat readStat(String queueName) {
        // 这里是文本直接使用 Scanner 读
        Stat stat = new Stat();
        try (InputStream is = new FileInputStream(getQueueStatPath(queueName))) {
            Scanner sc = new Scanner(is);
            stat.totalCount = sc.nextInt();
            stat.validCount = sc.nextInt();
            sc.close();
            return stat;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private void writeStat(String queueName, Stat stat) {
        try (OutputStream outputStream = new FileOutputStream(getQueueStatPath(queueName), true)) {
            PrintWriter printWriter = new PrintWriter(outputStream);
            printWriter.write(stat.totalCount + "\t" + stat.validCount + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    // 创建队列对应的目录和文件
    public void createQueueFiles(String queueName) throws IOException {
        // 1.创建文件所在目录
        File baseDir = new File(getQueueDir(queueName));
        if (!baseDir.exists()) {
            boolean ok = baseDir.mkdirs();
            if (!ok) {
                throw new IOException("Unable to create directory " + baseDir.getAbsolutePath());
            }
        }

        // 2.创建队列数据文件
        File queueDataFile = new File(getQueueDataPath(queueName));
        if (!queueDataFile.exists()) {
            boolean ok = queueDataFile.createNewFile();
            if (!ok) {
                throw new IOException("Unable to create queueDataFile " + queueDataFile.getAbsolutePath());
            }
        }

        // 3.创建消息统计文件
        File queueStatFile = new File(getQueueStatPath(queueName));
        if (!queueStatFile.exists()) {
            boolean ok = queueStatFile.createNewFile();
            if (!ok) {
                throw new IOException("Unable to create queueStatFile " + queueStatFile.getAbsolutePath());
            }
        }

        // 4.给消息统计文件设置初始值 0 \t 0
        Stat stat = new Stat();
        stat.totalCount = 0;
        stat.validCount = 0;
        writeStat(queueName, stat);
    }

    // 删除队列对应的目录和文件
    // 删除队列之后队列所对应的消息文件也随之删除
    public void destroyQueueFiles(String queueName) throws IOException {
        File queueDataFile = new File(getQueueDataPath(queueName));
        boolean ok1 = queueDataFile.delete();
        File queueStatFile = new File(getQueueStatPath(queueName));
        boolean ok2 = queueStatFile.delete();
        File baseDir = new File(getQueueDir(queueName));
        boolean ok3 = baseDir.delete();
        if(!ok1 || !ok2 || !ok3){
            throw new IOException("delete dir and files failed" + queueStatFile.getAbsolutePath());
        }
    }

    // 检查队列对应的文件是否存在
    public boolean checkFilesExists(String queueName) {
        // 检查数据文件和统计文件是否都存在
        File queueDataFile = new File(getQueueDataPath(queueName));
        
        File queueStatFile = new File(getQueueStatPath(queueName));

        return queueDataFile.exists() && queueStatFile.exists();
    }

}
