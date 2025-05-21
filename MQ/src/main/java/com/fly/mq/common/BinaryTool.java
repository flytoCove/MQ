package com.fly.mq.common;

import java.io.*;

public class BinaryTool {

    // 将一个对象序列化成一个字节数组
    public static byte[] toBytes(Object object) throws IOException {
        // 相当于一个变长的字节数组
        // 就可以把一个 Object 序列化的数据逐渐的写入 ByteArrayOutputStream 流对象 在统一转成 byte[]
        try(ByteArrayOutputStream baos = new ByteArrayOutputStream()){
            try(ObjectOutputStream oos = new ObjectOutputStream(baos)){
                // writeObject 会把 object 序列化 生成的二进制数据就会写入 ObjectOutputStream 中
                // ObjectOutputStream 关联到了 ByteArrayOutputStream 所以最终结果就写入到了 ByteArrayOutputStream
                oos.writeObject(object);
            }

            // 把 ByteArrayOutputStream 持有的二进制数据取出来转成 byte[]
            return baos.toByteArray();
        }
    }

    // 将一个字节数组反序列化成一个对象
    public static Object fromBytes(byte[] data) throws IOException, ClassNotFoundException {
        Object object = null;
        try(ByteArrayInputStream bais = new ByteArrayInputStream(data)){
            try(ObjectInputStream ois = new ObjectInputStream(bais)){
                // readObject 从 data 这个 byte[] 中读取数据进行反序列化
                object = ois.readObject();
            }
        }
        return object;
    }
}
