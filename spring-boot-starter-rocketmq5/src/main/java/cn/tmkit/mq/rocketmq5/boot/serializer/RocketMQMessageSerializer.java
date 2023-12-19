package cn.tmkit.mq.rocketmq5.boot.serializer;

import org.jetbrains.annotations.NotNull;

/**
 * 消息序列化接口：定义了如何序列化和反序列化
 *
 * @param <T>消息类型
 * @author ming.tang
 * @version 0.0.1
 * @date 2023-12-18
 */
public interface RocketMQMessageSerializer<T> {

    /**
     * 将给定的对象序列为二进制内容
     *
     * @param payload 给定的对象
     * @return 二进制内容
     */
    byte[] serialize(@NotNull T payload);

    /**
     * 将二进制内容反序列化为对象
     *
     * @param data 二进制内容
     * @return 反序列化后的对象
     */
    @NotNull T deserialize(byte[] data);

    /**
     * 基于JDK序列化的消息序列化器
     *
     * @return JDK序列化的消息序列化器
     */
    static RocketMQMessageSerializer<Object> java() {
        return new JdkSerializationRocketMQMessageSerializer();
    }

    /**
     * 基于Jackson 2序列化的消息序列化器
     *
     * @return Jackson 2序列化的消息序列化器
     */
    static RocketMQMessageSerializer<Object> jackson() {
        return new GenericJackson2JsonRocketMQMessageSerializer();
    }

}
