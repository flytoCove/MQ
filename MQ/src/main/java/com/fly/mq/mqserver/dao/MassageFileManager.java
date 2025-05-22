package com.fly.mq.mqserver.dao;

import com.fly.mq.common.BinaryTool;
import com.fly.mq.common.MQException;
import com.fly.mq.mqserver.core.MSGQueue;
import com.fly.mq.mqserver.core.Message;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.LinkedList;
import java.util.Scanner;

/**
 * 针对消息管理
 */
@Slf4j
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
        return getQueueDir(queueName) + "/queue_data.txt";
    }

    // 用来获取该队列的消息统计文件
    private String getQueueStatPath(String queueName) {
        return getQueueDir(queueName) + "/queue_stat.txt";
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
        try (OutputStream outputStream = new FileOutputStream(getQueueStatPath(queueName))) {
            PrintWriter printWriter = new PrintWriter(outputStream);
            printWriter.write(stat.totalCount + "\t" + stat.validCount);
            printWriter.flush();
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
            throw new IOException("delete dir and files failed " + queueStatFile.getAbsolutePath());
        }
    }

    // 检查队列对应的文件是否存在
    public boolean checkFilesExists(String queueName) {
        // 检查数据文件和统计文件是否都存在
        File queueDataFile = new File(getQueueDataPath(queueName));

        File queueStatFile = new File(getQueueStatPath(queueName));

        return queueDataFile.exists() && queueStatFile.exists();
    }

    // 写入消息
    // 将新的消息放到对应的 queue 对应的文件中
    public void sendMessage(MSGQueue queue, Message message) throws IOException {
        // 1.检查当前队列对应的文件是否存在
        if(!checkFilesExists(queue.getName())){
            throw new MQException("[MassageFileManager] File does not exist: " + queue.getName());
        }

        // 2.将 Message 进行序列化转成字节数组
        byte[] binaryMessage = BinaryTool.toBytes(message);

        // 3.获取到队列数据文件的长度 计算出该 Message对象的 offsetBeg 和 offsetEnd
        // 把新的 message 写到队列数据文件的末尾 此时 Message 对象的 offsetEnd 为当前数据文件长度 + 4
        // offsetEnd 为当前文件长度 + 4 加自身数据长度

        // 针对 queue 进行加锁保证多个线程向同一个队列写入文件时的线程安全
        synchronized(queue) {
            File queueDataFile = new File(getQueueDataPath(queue.getName()));
            message.setOffsetBeg(queueDataFile.length() + 4);
            message.setOffsetEnd(queueDataFile.length() + 4 + binaryMessage.length);

            // 4.写入消息到数据文件
            try (OutputStream fos = new FileOutputStream(queueDataFile, true)) {
                try (DataOutputStream dos = new DataOutputStream(fos)) {
                    // 1.先写入 4 个字节的消息长度
                    dos.writeInt(binaryMessage.length);
                    // 2.写入消息
                    dos.write(binaryMessage);
                }
            }

            // 5.更新统计消息文件
            Stat stat = readStat(queue.getName());
            stat.totalCount++;
            stat.validCount++;
            writeStat(queue.getName(), stat);
        }
    }

    // 删除消息 isValid 设为 0
    public void deleteMessage(MSGQueue queue, Message message) throws IOException, ClassNotFoundException {
        synchronized (queue) {
            try (RandomAccessFile raf = new RandomAccessFile(getQueueDataPath(queue.getName()), "rw")) {
                // 1.从文件中读出二进制数据
                // seek 控制光标指向
                byte[] srcBuffer = new byte[(int) (message.getOffsetEnd() - message.getOffsetBeg())];
                raf.seek(message.getOffsetBeg());
                raf.read(srcBuffer);

                // 2.将二进制数据转成 Message 对象
                Message diskMessage = (Message) BinaryTool.fromBytes(srcBuffer);

                // 3.将isValid 设置设置成 0x0（无效）
                diskMessage.setIsValid((byte) 0x0);

                // 4.重新写入文件
                byte[] destBuffer = BinaryTool.toBytes(diskMessage);
                raf.seek(message.getOffsetBeg());
                raf.write(destBuffer);
            }

            Stat stat = readStat(queue.getName());
            if (stat.validCount > 0) {
                stat.validCount--;
            }
            writeStat(queue.getName(), stat);
        }
    }

    /**
     * 从加载所有的消息内容到内存 服务启动时调用
     * @param queueName
     * @return
     * @throws IOException
     */
    public LinkedList<Message> loadAllMessageFromQueue(String queueName) throws IOException, ClassNotFoundException {
        LinkedList<Message> messages = new LinkedList<>();
        try(InputStream stream = new FileInputStream(getQueueDataPath(queueName))) {
            try(DataInputStream dis = new DataInputStream(stream)) {
                long currentOffset = 0;
                while(true) {
                    // 1.读取当前消息长度，可能读到文件末尾，会抛出 EOPException
                    int messageSize = dis.readInt();

                    // 2.按照这个消息长度读取消息内容
                    byte[] messageBytes = new byte[messageSize];
                    if(dis.read(messageBytes) != messageSize){
                        // 如果不匹配说明文件有错误
                        throw new MQException("[MassageFileManager] Message size mismatch");
                    }

                    // 3.将读到的二进制消息反序列化成 Message 对象
                    Message message = (Message) BinaryTool.fromBytes(messageBytes);

                    // 4.如果为无效消息直接跳过
                    if(message.getIsValid() != 0x1){
                        currentOffset += (4 + messageBytes.length);
                        continue;
                    }

                    // 5.将消息加入到链表中
                    message.setOffsetBeg(currentOffset + 4);
                    message.setOffsetEnd(currentOffset + 4 + messageBytes.length);
                    currentOffset += (4 + messageBytes.length);
                    messages.add(message);
                }
            }catch (EOFException e){
                // 处理文件读取到末尾
            }
        }
        return messages;
    }

    /**
     * 检查当前是否要针对该队列的数据文件进行 GC
     * @return
     */
    public boolean checkGC(String queueName) {
        // 根据总消息数和有效消息数判断是否需要 GC
        Stat stat = readStat(queueName);
        assert stat != null;
        return stat.totalCount > 2000 && (double) stat.validCount / (double) stat.totalCount < 0.5;
    }

    // 获取新文件路径
    private String getQueueDataNewPath(String queueName) {
        return getQueueDataPath(queueName) + "queue_data_new.txt";
    }

    /**
     * 真正垃圾回收 使用复制算法完成
     * 创建一个新的文件 queue_data_new.txt 将原来数据文件里的有效数据读取出来写到新的文件中
     * 删除旧的文件
     * 在将文件重命名成原来的 queue_data.txt 同时更新消息统计文件
     * @param queue
     */
    public void gc(MSGQueue queue) throws IOException, ClassNotFoundException {
        synchronized (queue) {
            // 此处统计一下 gc 的耗时
            long gcBeg = System.currentTimeMillis();

            // 1.创建一个新的文件
            File queueDataNewFile = new File(getQueueDataNewPath(queue.getName()));
            if(queueDataNewFile.exists()){
                throw new MQException("[MassageFileManager] File already exists: " + queue.getName());
            }
            boolean ok = queueDataNewFile.createNewFile();
            if(!ok){
                throw new IOException("[MassageFileManager] Create file fail: " + queueDataNewFile.getAbsolutePath());
            }

            // 2.从旧的文件中读取出所有消息
            LinkedList<Message> messages = loadAllMessageFromQueue(queue.getName());

            // 3.将有效消息写到新的文件
            try(OutputStream fos = new FileOutputStream(queueDataNewFile, true)) {
                try(DataOutputStream dos = new DataOutputStream(fos)) {
                    for (Message message : messages) {
                        byte[] buffer = BinaryTool.toBytes(message);
                        dos.writeInt(buffer.length);
                        dos.write(buffer);
                    }
                }
            }

            // 4.删除旧的数据文件
            File oldFile = new File(getQueueDataPath(queue.getName()));
            ok = oldFile.delete();
            if(!ok){
                throw new IOException("[MassageFileManager] Delete oldFile fail: " + oldFile.getAbsolutePath());
            }
            ok = queueDataNewFile.renameTo(oldFile);
            if(!ok){
                throw new IOException("[MassageFileManager] Rename fail: " + queueDataNewFile.getAbsolutePath());
            }

            // 5.更新统计文件
            Stat stat = readStat(queue.getName());
            assert stat != null;
            stat.totalCount = messages.size();
            stat.validCount = messages.size();
            writeStat(queue.getName(), stat);

            long gcEnd = System.currentTimeMillis();

            log.info("[MassageFileManager] GC completed in {}ms", gcEnd - gcBeg);

        }
    }


}
